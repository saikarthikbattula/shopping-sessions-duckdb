//! Scan complete CSV rows directly from a buffer. The competition dialect has
//! no commas/newlines inside values, so structural bytes can be classified in
//! blocks before validating each field's surrounding quotes.

struct Row<'a, const N: usize> {
    fields: [Option<&'a [u8]>; N],
    line_start: usize,
    field_start: usize,
    field_count: usize,
    quotes: usize,
    valid: bool,
}

impl<'a, const N: usize> Row<'a, N> {
    fn new() -> Self {
        Self {
            fields: [None; N], line_start: 0, field_start: 0, field_count: 0,
            quotes: 0, valid: true,
        }
    }

    #[inline(always)]
    fn delimiter<E>(
        &mut self,
        buf: &'a [u8],
        position: usize,
        consume: &mut impl FnMut(&[Option<&'a [u8]>; N]) -> Result<(), E>,
    ) -> Result<(), E> {
        let byte = buf[position];
        if byte == b'"' {
            self.quotes += 1;
            return Ok(());
        }

        if self.field_count < N {
            let mut start = self.field_start;
            let mut end = position;
            if self.quotes != 0 {
                if self.quotes == 2 && buf[start] == b'"' && buf[end - 1] == b'"' {
                    start += 1;
                    end -= 1;
                } else {
                    self.valid = false;
                }
            }
            self.fields[self.field_count] = if start == end { None } else { Some(&buf[start..end]) };
        } else {
            self.valid = false;
        }
        self.field_count += 1;
        self.field_start = position + 1;
        self.quotes = 0;

        if byte == b'\n' {
            if self.valid && self.field_count == N && position > self.line_start {
                consume(&self.fields)?;
            }
            self.line_start = position + 1;
            self.field_count = 0;
            self.valid = true;
        }
        Ok(())
    }
}

// An exact per-byte equality mask. The common subtract-one zero-byte idiom
// borrows across bytes and can flag a neighbouring byte equal to target ^ 1.
// Restricting addition to seven bits prevents those false delimiter positions.
#[inline(always)]
fn equal_bytes(word: u64, byte: u8) -> u64 {
    const LOW: u64 = 0x7f7f_7f7f_7f7f_7f7f;
    let value = word ^ u64::from(byte) * 0x0101_0101_0101_0101;
    !((value & LOW).wrapping_add(LOW) | value | LOW) & 0x8080_8080_8080_8080
}

#[inline(always)]
fn delimiters<'a, E, const N: usize, const SHIFT: u32>(
    row: &mut Row<'a, N>,
    buf: &'a [u8],
    base: usize,
    mut mask: u64,
    consume: &mut impl FnMut(&[Option<&'a [u8]>; N]) -> Result<(), E>,
) -> Result<(), E> {
    while mask != 0 {
        let offset = (mask.trailing_zeros() >> SHIFT) as usize;
        row.delimiter(buf, base + offset, consume)?;
        mask &= mask - 1;
    }
    Ok(())
}

#[inline(always)]
fn remainder<'a, E, const N: usize>(
    row: &mut Row<'a, N>,
    buf: &'a [u8],
    mut position: usize,
    consume: &mut impl FnMut(&[Option<&'a [u8]>; N]) -> Result<(), E>,
) -> Result<(), E> {
    while position + 8 <= buf.len() {
        // The loop guarantees all eight loaded bytes belong to the buffer.
        let word = unsafe { (buf.as_ptr().add(position) as *const u64).read_unaligned().to_le() };
        let mask = equal_bytes(word, b',') | equal_bytes(word, b'"') | equal_bytes(word, b'\n');
        delimiters::<E, N, 3>(row, buf, position, mask, consume)?;
        position += 8;
    }
    for index in position..buf.len() {
        if matches!(buf[index], b',' | b'"' | b'\n') {
            row.delimiter(buf, index, consume)?;
        }
    }
    Ok(())
}

fn scalar<'a, E, const N: usize>(
    buf: &'a [u8],
    consume: &mut impl FnMut(&[Option<&'a [u8]>; N]) -> Result<(), E>,
) -> Result<usize, E> {
    let mut row = Row::new();
    remainder(&mut row, buf, 0, consume)?;
    Ok(row.line_start)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2<'a, E, const N: usize>(
    buf: &'a [u8],
    consume: &mut impl FnMut(&[Option<&'a [u8]>; N]) -> Result<(), E>,
) -> Result<usize, E> {
    use std::arch::x86_64::*;
    // The caller checks AVX2 support before entering this kernel.
    let (comma, quote, newline) = unsafe {
        (_mm256_set1_epi8(b',' as i8), _mm256_set1_epi8(b'"' as i8), _mm256_set1_epi8(b'\n' as i8))
    };
    let mut row = Row::new();
    let mut position = 0;
    while position + 32 <= buf.len() {
        let bytes = unsafe { _mm256_loadu_si256(buf.as_ptr().add(position) as *const __m256i) };
        let mask = unsafe {
            let structural = _mm256_or_si256(
                _mm256_cmpeq_epi8(bytes, comma),
                _mm256_or_si256(_mm256_cmpeq_epi8(bytes, quote), _mm256_cmpeq_epi8(bytes, newline)),
            );
            _mm256_movemask_epi8(structural) as u32
        };
        delimiters::<E, N, 0>(&mut row, buf, position, u64::from(mask), consume)?;
        position += 32;
    }
    remainder(&mut row, buf, position, consume)?;
    Ok(row.line_start)
}

/// Emit well-formed rows ending in LF and return the start of the unfinished
/// final row. No slice outlives `buf`; callback errors propagate immediately.
pub fn scan_rows<'a, E, const N: usize>(
    buf: &'a [u8],
    mut consume: impl FnMut(&[Option<&'a [u8]>; N]) -> Result<(), E>,
) -> Result<usize, E> {
    #[cfg(target_arch = "x86_64")]
    if is_x86_feature_detected!("avx2") {
        // Feature detection happens once per buffer, outside the row loop.
        return unsafe { avx2(buf, &mut consume) };
    }
    scalar(buf, &mut consume)
}

#[cfg(test)]
mod tests {
    use super::*;

    type Fields = Vec<Option<Vec<u8>>>;

    fn reference<const N: usize>(buf: &[u8]) -> (Vec<Fields>, usize) {
        let tail = buf.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1);
        let mut rows = Vec::new();
        if tail == 0 { return (rows, tail); }
        for line in buf[..tail - 1].split(|&b| b == b'\n') {
            if line.is_empty() { continue; }
            let raw: Vec<_> = line.split(|&b| b == b',').collect();
            if raw.len() != N { continue; }
            let mut fields = Vec::new();
            let mut valid = true;
            for value in raw {
                let inner = if value.first() == Some(&b'"') {
                    if value.len() < 2 || value.last() != Some(&b'"') {
                        valid = false;
                        break;
                    }
                    &value[1..value.len() - 1]
                } else { value };
                if inner.contains(&b'"') {
                    valid = false;
                    break;
                }
                fields.push(if inner.is_empty() { None } else { Some(inner.to_vec()) });
            }
            if valid { rows.push(fields); }
        }
        (rows, tail)
    }

    fn check<const N: usize>(buf: &[u8]) {
        let expected = reference::<N>(buf);
        let mut actual = Vec::new();
        let tail = scan_rows::<(), N>(buf, |fields| {
            actual.push(fields.iter().map(|field| field.map(<[u8]>::to_vec)).collect::<Fields>());
            Ok(())
        }).unwrap();
        assert_eq!((actual, tail), expected);
        // On AVX2 hosts the public function exercises SIMD; explicitly retain
        // parity coverage for the portable word-at-a-time implementation.
        let mut portable = Vec::new();
        let tail = scalar::<(), N>(buf, &mut |fields| {
            portable.push(fields.iter().map(|field| field.map(<[u8]>::to_vec)).collect::<Fields>());
            Ok(())
        }).unwrap();
        assert_eq!((portable, tail), expected);
    }

    #[test]
    fn exact_masks_reject_adjacent_near_delimiters() {
        for delimiter in [b',', b'"', b'\n'] {
            for value in 0..=255u8 {
                for position in 0..8 {
                    let mut bytes = [delimiter; 8];
                    bytes[position] = value;
                    let mask = equal_bytes(u64::from_le_bytes(bytes), delimiter);
                    for i in 0..8 {
                        assert_eq!((mask >> (i * 8 + 7)) & 1, u64::from(bytes[i] == delimiter));
                    }
                }
            }
        }
    }

    #[test]
    fn quotes_nulls_cr_and_partial_rows_match_reference() {
        for buf in [
            &b"a,b\n\"a\",\"b\"\n,\"\"\n \"a\",b\na\",b\n\"a\"x,b\n"[..],
            &b"\"a\"\"b\",c\n\"a,b\",c\n\"unterminated,b\nnext,row\n"[..],
            &b"\"a\r\",b\r\na,b\nlast,partial"[..],
            &b"\n\n,,\n,a\n\"\",\"\"\n\0,\0\n\"\0\",\"\0\"\n"[..],
        ] {
            check::<1>(buf);
            check::<2>(buf);
            check::<4>(buf);
            check::<7>(buf);
        }
    }

    #[test]
    fn delimiters_and_quotes_across_block_boundaries() {
        for length in [0, 1, 6, 7, 8, 9, 14, 15, 16, 17, 30, 31, 32, 33, 63, 64, 65, 511, 512, 513, 50000] {
            let mut buf = vec![b'a'; length];
            buf.extend_from_slice(b",\"b\"\n\"c\",d\n\"a\"\"\",b\nlast,");
            check::<2>(&buf);
        }
    }

    #[test]
    fn randomized_structural_bytes_match_reference() {
        let alphabet = b"ab,\"\n\r -!\0";
        let mut random = 0x1234_5678_9abc_def0u64;
        for case in 0..1000 {
            let mut buf = Vec::new();
            for _ in 0..(case * 17 % 4096) {
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                buf.push(alphabet[(random >> 32) as usize % alphabet.len()]);
            }
            check::<2>(&buf);
            check::<4>(&buf);
            check::<7>(&buf);
        }
    }

    #[test]
    fn callback_failure_stops_scanning() {
        let mut calls = 0;
        let result = scan_rows::<_, 2>(b"a,b\nc,d\n", |_| {
            calls += 1;
            Err("stop")
        });
        assert_eq!(result, Err("stop"));
        assert_eq!(calls, 1);
    }
}
