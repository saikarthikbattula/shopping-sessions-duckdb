//! Counting scatter by person, then timestamp/byte-wise-ID sorting within each row.
use super::{LocalClick, LocalEvent};
pub(super) struct ClickIndex { pub clicks: Vec<LocalClick>, pub offsets: Vec<usize> }
impl ClickIndex {
    pub(super) fn build(input: &[LocalClick], people: usize) -> Self {
        let mut offsets = vec![0; people+1];
        for click in input { offsets[click.p_idx as usize+1] += 1; }
        for i in 1..offsets.len() { offsets[i] += offsets[i-1]; }
        let mut clicks = input.to_vec();
        let mut next = offsets[..people].to_vec();
        for click in input {
            let p = click.p_idx as usize;
            clicks[next[p]] = *click;
            next[p] += 1;
        }
        for p in 0..people {
            clicks[offsets[p]..offsets[p+1]].sort_unstable_by(|a,b|
                a.ts_ms.cmp(&b.ts_ms).then_with(|| a.click_id.cmp(&b.click_id)));
        }
        Self { clicks, offsets }
    }
}
// Group events by numeric person with a counting scatter. Sort only each
// person's timestamps: no repeated string or person-key comparisons.
pub(super) fn sort_events(events: &mut Vec<LocalEvent>, scratch: &mut Vec<LocalEvent>, people: usize) {
    let Some(&first) = events.first() else { return; };
    let mut offsets = vec![0; people+1];
    for event in events.iter() { offsets[event.p_idx as usize+1] += 1; }
    for p in 1..offsets.len() { offsets[p] += offsets[p-1]; }
    scratch.resize(events.len(), first);
    let mut next = offsets[..people].to_vec();
    for event in events.iter() {
        let p = event.p_idx as usize;
        scratch[next[p]] = *event;
        next[p] += 1;
    }
    for p in 0..people {
        scratch[offsets[p]..offsets[p+1]].sort_unstable_by_key(|e| e.ts_ms);
    }
    std::mem::swap(events, scratch);
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::CompactId;
    #[test]
    fn event_counting_sort_matches_person_timestamp_order_and_reuses_buffers() {
        let mut scratch = Vec::new();
        for n in [0, 1, 500, 2, 2000] {
            let mut input: Vec<_> = (0..n).map(|i| LocalEvent {
                p_idx: ((i*31) % 17) as u32, ts_ms: ((i*73) % 101) as i64,
                price_category: 123,
            }).collect();
            let mut expected: Vec<_> = input.iter().map(|e| (e.p_idx,e.ts_ms,e.price_category)).collect();
            expected.sort_unstable();
            sort_events(&mut input,&mut scratch,19);
            assert_eq!(input.iter().map(|e| (e.p_idx,e.ts_ms,e.price_category)).collect::<Vec<_>>(),expected);
        }
    }
    #[test]
    fn csr_empty_rows_and_bytewise_ties() {
        let c = |p,ts,id: &[u8]| LocalClick { p_idx:p, ts_ms:ts, click_id:CompactId::from_bytes(id),camp_idx:0 };
        let index = ClickIndex::build(&[c(2,1,b"z"),c(0,2,b"b"),c(2,1,b"A"),c(0,1,b"x")],4);
        assert_eq!(index.offsets,vec![0,2,2,4,4]);
        assert_eq!(index.clicks.iter().map(|c| c.click_id.as_str()).collect::<Vec<_>>(),vec!["x","b","A","z"]);
        assert_eq!(ClickIndex::build(&[],0).offsets,vec![0]);
    }
}
