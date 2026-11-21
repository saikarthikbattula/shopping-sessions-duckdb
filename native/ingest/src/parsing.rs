pub fn line(input: &str, count: usize) -> Option<Vec<Option<&str>>> {
    let input = input.strip_suffix('\n').unwrap_or(input);
    if input.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(count);
    for field in input.split(',') {
        let value = if field.contains('"') {
            let inner = field.strip_prefix('"')?.strip_suffix('"')?;
            if inner.contains('"') {
                return None;
            }
            inner
        } else {
            field
        };
        out.push(if value.is_empty() { None } else { Some(value) });
    }
    (out.len() == count).then_some(out)
}
pub fn unsigned(value: Option<&str>, maximum: i64) -> Option<i64> {
    let value = value?;
    if value.is_empty() {
        return None;
    }
    let mut n = 0i64;
    for b in value.bytes() {
        if !b.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add(i64::from(b - b'0'))?;
        if n > maximum {
            return None;
        }
    }
    Some(n)
}
pub fn timestamp(value: Option<&str>) -> Option<i64> {
    let s = value?;
    if s.is_empty() {
        return None;
    }
    let b = s.as_bytes();
    if b.iter().all(u8::is_ascii_digit) {
        return unsigned(Some(s), i64::MAX);
    }
    if b.len() != 20 && b.len() != 25 {
        return None;
    }
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let number = |a: usize, z: usize| -> Option<i64> {
        let mut n = 0;
        for &v in &b[a..z] {
            if !v.is_ascii_digit() {
                return None;
            }
            n = n * 10 + i64::from(v - b'0');
        }
        Some(n)
    };
    let (y, m, d, h, mi, se) = (
        number(0, 4)?,
        number(5, 7)?,
        number(8, 10)?,
        number(11, 13)?,
        number(14, 16)?,
        number(17, 19)?,
    );
    if y == 0 || !(1..=12).contains(&m) || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let mdays = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if d < 1 || d > mdays[(m - 1) as usize] {
        return None;
    }
    let offset = if b.len() == 20 {
        if b[19] != b'Z' {
            return None;
        }
        0
    } else {
        if (b[19] != b'+' && b[19] != b'-') || b[22] != b':' {
            return None;
        }
        let (oh, om) = (number(20, 22)?, number(23, 25)?);
        if oh > 23 || om > 59 {
            return None;
        }
        (oh * 60 + om) * if b[19] == b'+' { 1 } else { -1 }
    };
    let prior = y - 1;
    let ordinal = 365 * prior + prior / 4 - prior / 100
        + prior / 400
        + mdays[..(m - 1) as usize].iter().sum::<i64>()
        + d;
    Some(((ordinal - 719163) * 86400 + h * 3600 + mi * 60 + se - offset * 60) * 1000)
}
pub fn window(value: Option<&str>, start: i128, end: i128) -> Option<i64> {
    timestamp(value).filter(|&v| start <= i128::from(v) && i128::from(v) < end)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dialect() {
        assert_eq!(
            line("\" u\",\"\",x\r\n", 3),
            Some(vec![Some(" u"), None, Some("x\r")])
        );
        for v in ["", "a", "a,b,c", "\"a,b", "a\"x,b", "\"a\"x,b", "\"a\"\",b"] {
            assert!(line(v, 2).is_none(), "{v}");
        }
        assert_eq!(line("é,中", 2), Some(vec![Some("é"), Some("中")]));
    }
    #[test]
    fn numbers() {
        assert_eq!(
            unsigned(Some(&format!("{}250", "0".repeat(10000))), 10000000),
            Some(250)
        );
        assert_eq!(unsigned(Some("000"), 10000000), Some(0));
        assert_eq!(unsigned(Some("10000000"), 10000000), Some(10000000));
        for s in ["", "٢٥٠", "２５０", "+1", "-1", "1\r", " 1", "10000001"] {
            assert_eq!(unsigned(Some(s), 10000000), None);
        }
        assert_eq!(timestamp(Some("9223372036854775807")), Some(i64::MAX));
        assert_eq!(timestamp(Some("9223372036854775808")), None);
        assert_eq!(timestamp(Some(&"9".repeat(10000))), None);
    }
    #[test]
    fn calendars() {
        for s in [
            "2026-10-03T23:58:10Z",
            "2026-10-03T19:58:10-04:00",
            "2026-10-04T01:58:10+02:00",
            "2026-10-03T23:58:10-00:00",
        ] {
            assert_eq!(timestamp(Some(s)), Some(1791071890000));
        }
        assert_eq!(timestamp(Some("2028-02-29T00:00:00Z")), Some(1835395200000));
        assert_eq!(
            timestamp(Some("1970-01-01T00:00:00+23:59")),
            Some(-86340000)
        );
        for s in [
            "0000-01-01T00:00:00Z",
            "2027-02-29T00:00:00Z",
            "2028-02-30T00:00:00Z",
            "2026-13-01T00:00:00Z",
            "2026-01-00T00:00:00Z",
            "2026-10-03T24:00:00Z",
            "2026-10-03T23:60:00Z",
            "2026-10-03T23:59:60Z",
            "2026-10-03T00:00:00+24:00",
            "2026-10-03T00:00:00+05:60",
            "2026-10-03T00:00:00+0200",
            "2026-10-03t00:00:00Z",
            "2026-10-03T00:00:00z",
            "2026-10-03T00:00:00.001Z",
            "2026-10-03T00:00:00Z\r",
        ] {
            assert_eq!(timestamp(Some(s)), None, "{s}");
        }
    }
    #[test]
    fn windows() {
        for (s, e) in [(100, 200), (-604800000, 200)] {
            for (v, want) in [(s - 1, None), (s, Some(s)), (e - 1, Some(e - 1)), (e, None)] {
                if v >= 0 {
                    assert_eq!(window(Some(&v.to_string()), s as i128, e as i128), want);
                }
            }
        }
        assert_eq!(
            window(Some("0"), i128::from(i64::MIN) - 604800000, 1),
            Some(0)
        );
    }
}
