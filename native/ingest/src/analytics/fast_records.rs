// Two spelling namespaces preserve exact case while allowing the generator's
// whole-ID uppercase dirt. Mixed-case and alternate encodings use the fallback.
#[inline(always)]
pub(super) fn id_case(id: &[u8], prefix: u8) -> Option<u32> {
    if !(2..=9).contains(&id.len()) || (id.len() > 2 && id[1] == b'0') { return None; }
    if id[0] == prefix && id[1..].iter().all(|b| b.is_ascii_digit() || b.is_ascii_lowercase()) {
        Some(0)
    } else if id[0] == prefix.to_ascii_uppercase()
        && id[1..].iter().all(|b| b.is_ascii_digit() || b.is_ascii_uppercase()) {
        Some(1)
    } else { None }
}

// Checked, lossless packing for the generator path; rejection selects the exact pipeline.
pub(super) const EXTRA_PERSON: u32 = 1 << 20;

#[derive(Clone, Copy)]
pub(super) struct FastEvent {
    // [relative timestamp:31][price:24][category:8][valid:1]
    w0: u64,
    // [person token:21][relative ingestion seconds:21][local event ID:22]
    w1: u64,
}

impl FastEvent {
    pub(super) fn new(eid: u32, person: u32, ingest: i64, ts: i64, price: u32,
                      category: u8, valid: bool, start: i64) -> Option<Self> {
        let rel_ts = ts.checked_sub(start)?;
        // One day of headroom preserves early ingestion before the window.
        let rel_ingest = ingest.checked_sub(start.checked_sub(86_400_000)?)?;
        if !(0..1 << 31).contains(&rel_ts) || !(0..(1i64 << 21) * 1000).contains(&rel_ingest)
            || rel_ingest % 1000 != 0 || eid >= 1 << 22 || person >= 1 << 21 || price >= 1 << 24 {
            return None;
        }
        Some(Self {
            w0: (rel_ts as u64) << 33 | (price as u64) << 9 | (category as u64) << 1 | valid as u64,
            w1: (person as u64) << 43 | (rel_ingest as u64 / 1000) << 22 | eid as u64,
        })
    }
    // After global dedup the event ID and ingest time are dead. Reuse the low
    // bits for the parse worker owning an exceptional identity's side entry.
    pub(super) fn with_worker(mut self, worker: u16) -> Self {
        self.w1 = (self.w1 & !((1 << 22)-1)) | u64::from(worker);
        self
    }
    pub(super) fn worker(self) -> u16 { (self.w1 & 0xffff) as u16 }
    pub(super) fn local_eid(self) -> u32 { (self.w1 & ((1 << 22) - 1)) as u32 }
    pub(super) fn person_idx(self) -> u32 { (self.w1 >> 43) as u32 }
    pub(super) fn ingest_ms(self, start: i64) -> i64 {
        start - 86_400_000 + ((self.w1 >> 22) & ((1 << 21) - 1)) as i64 * 1000
    }
    pub(super) fn ts_ms(self, start: i64) -> i64 { start + (self.w0 >> 33) as i64 }
    pub(super) fn price(self) -> u32 { ((self.w0 >> 9) & 0xff_ffff) as u32 }
    pub(super) fn category(self) -> u8 { (self.w0 >> 1) as u8 }
    pub(super) fn valid(self) -> bool { self.w0 & 1 != 0 }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn packed_boundaries_and_rejections() {
        assert_eq!(std::mem::size_of::<FastEvent>(), 16);
        let start = 1_800_000_000_000;
        let r = FastEvent::new((1 << 22)-1, (1 << 21)-1, start-1000,
                              start+(1 << 31)-1, 10_000_000, 255, true, start).unwrap();
        assert_eq!(r.local_eid(), (1 << 22)-1);
        assert_eq!(r.person_idx(), (1 << 21)-1);
        assert_eq!(r.ts_ms(start), start+(1 << 31)-1);
        assert_eq!(r.ingest_ms(start), start-1000);
        assert_eq!(r.price(), 10_000_000);
        assert_eq!(r.category(), 255);
        assert!(r.valid());
        let survivor = r.with_worker(15);
        assert_eq!(survivor.worker(), 15);
        assert_eq!(survivor.person_idx(), r.person_idx());
        assert_eq!(survivor.ts_ms(start), r.ts_ms(start));
        assert_eq!(survivor.price(), r.price());
        for raw in [b"Eabc".as_slice(), b"e01", b"eABC", b"e", b"e123456789"] {
            assert!(id_case(raw, b'e').is_none());
        }
        assert_eq!(id_case(b"e0", b'e'), Some(0));
        assert_eq!(id_case(b"E0", b'e'), Some(1));
        assert_eq!(id_case(b"EABC", b'e'), Some(1));
        assert_eq!(id_case(b"eabc123", b'e'), Some(0));
        for (eid,p,ing,ts,price) in [(1 << 22,0,start,start,0), (0,1 << 21,start,start,0),
            (0,0,start+1,start,0), (0,0,start,start-1,0), (0,0,start,start+(1 << 31),0),
            (0,0,start-86_401_000,start,0), (0,0,start,start,1 << 24)] {
            assert!(FastEvent::new(eid,p,ing,ts,price,0,false,start).is_none());
        }
    }
}
