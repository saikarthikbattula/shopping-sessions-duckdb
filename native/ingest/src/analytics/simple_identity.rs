//! Direct identity resolution derived from the exact union-find result.
//! A user number alone never proves that its aliases are connected.
use super::{CompactId, FrozenIdentity, decode_b36, unscramble, SCRAMBLE_MASK, SCRAMBLE_M1, SCRAMBLE_M2, get_shard_bytes};
use super::fast_records::EXTRA_PERSON;

#[derive(Clone, Copy)]
struct User {
    canonical: CompactId,
    aliases: u16,
    shard: u8,
    seen: bool,
    complex: bool,
}
impl Default for User {
    fn default() -> Self {
        Self { canonical: CompactId::default(), aliases: 1, shard: 0, seen: false, complex: false }
    }
}

pub(super) struct SimpleUsers { users: Vec<User>, key: u64 }

// Reject case variants, leading zeros, oversized integers and alternate encodings.
fn decode(user: &[u8], key: u64) -> Option<(usize, usize)> {
    if user.first() != Some(&b'u') || user.len() < 2 || user.len() > 9
        || (user.len() > 2 && user[1] == b'0')
        || !user[1..].iter().all(|b| b.is_ascii_digit() || b.is_ascii_lowercase()) { return None; }
    let n = decode_b36(&user[1..])?;
    if n > SCRAMBLE_MASK { return None; }
    let n = unscramble(n, key);
    let u = (n >> 9) as usize;
    if u >= EXTRA_PERSON as usize { return None; }
    Some((u, (n & 511) as usize))
}

impl SimpleUsers {
    pub(super) fn build(ids: &FrozenIdentity, key: u64) -> Self {
        let mut result = Self { users: Vec::new(), key };
        // Frozen entries contain precisely the non-self mappings from the full
        // disjoint set, including unused and uppercase canonical names.
        for slot in &ids.short.slots {
            if slot.key_len != 0 {
                result.add(&slot.key[..slot.key_len as usize], &slot.canonical[..slot.canonical_len as usize]);
            }
        }
        for shard in &ids.shards {
            for (alias, person) in shard { result.add(alias, person.id.as_bytes()); }
        }
        // Cache singleton alias-0 names too, so event survivors need no encoding
        // or string hashing during deduplication and analytics.
        for (u, entry) in result.users.iter_mut().enumerate() {
            if !entry.seen {
                entry.canonical = encoded_user((u as u64) << 9, key);
                entry.shard = get_shard_bytes(entry.canonical.as_bytes()) as u8;
                entry.seen = true;
            }
        }
        eprintln!("[simple_identity] indexed_users: {}, complex_users: {}", result.users.len(),
                  result.users.iter().filter(|u| u.complex).count());
        result
    }
    pub(super) fn len(&self) -> usize { self.users.len() }
    fn add(&mut self, alias: &[u8], canonical: &[u8]) {
        let Some((u, a)) = decode(alias, self.key) else { return; };
        if self.users.len() <= u { self.users.resize(u+1, User::default()); }
        let Some((cu, ca)) = decode(canonical, self.key) else {
            self.users[u].complex = true;
            return;
        };
        if cu != u {
            if self.users.len() <= cu { self.users.resize(cu+1, User::default()); }
            self.users[u].complex = true;
            self.users[cu].complex = true;
            return;
        }
        let entry = &mut self.users[u];
        if a >= 8 || ca >= 8 { entry.complex = true; return; }
        let canonical = CompactId::from_bytes(canonical);
        if entry.seen && entry.canonical != canonical { entry.complex = true; return; }
        if !entry.seen {
            entry.seen = true;
            entry.aliases = 0;
            entry.canonical = canonical;
            entry.shard = get_shard_bytes(canonical.as_bytes()) as u8;
        }
        entry.aliases |= (1 << a) | (1 << ca);
    }
    #[inline(always)]
    pub(super) fn index(&self, user: &[u8]) -> Option<u32> {
        let (u, a) = decode(user, self.key)?;
        let entry = self.users.get(u)?;
        if entry.complex || a >= 8 || entry.aliases & (1 << a) == 0 { None } else { Some(u as u32) }
    }
    pub(super) fn person(&self, u: u32) -> (CompactId, u8) {
        let entry = self.users[u as usize];
        (entry.canonical, entry.shard)
    }
    #[inline(always)]
    pub(super) fn resolve(&self, user: &[u8], ids: &FrozenIdentity) -> (CompactId, u8) {
        match self.index(user) { Some(u) => self.person(u), None => ids.get_compact_with_shard(user) }
    }
}

fn encoded_user(n: u64, key: u64) -> CompactId {
    let x = n.wrapping_mul(SCRAMBLE_M1) & SCRAMBLE_MASK;
    let x = (x ^ key) & SCRAMBLE_MASK;
    let mut x = (x ^ (x >> 17)).wrapping_mul(SCRAMBLE_M2) & SCRAMBLE_MASK;
    let mut bytes = [0u8; 16];
    let mut i = bytes.len();
    loop {
        i -= 1;
        bytes[i] = b"0123456789abcdefghijklmnopqrstuvwxyz"[(x % 36) as usize];
        x /= 36;
        if x == 0 { break; }
    }
    i -= 1; bytes[i] = b'u';
    CompactId::from_bytes(&bytes[i..])
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn direct_lookup_preserves_split_aliases_case_and_shared_devices() {
        let key = 12345;
        let mut ids = FrozenIdentity::empty();
        ids.short = super::super::ShortIdentityTable::with_entries(8);
        let id = |u: u64,a: u64| encoded_user((u << 9) | a, key);
        let mut insert = |a: CompactId,b: CompactId| ids.short.insert(a.as_bytes(),b.as_bytes(),super::super::identity_hash(a.as_bytes()));
        insert(id(2,1),id(2,0));
        insert(id(2,3),id(2,4)); // disconnected second tree => complex
        insert(id(3,1),id(3,0));
        insert(id(4,1),id(3,0)); // shared component => fallback
        insert(id(5,1),CompactId::from_bytes(b"UPPERCASE"));
        insert(id(6,1),id(6,0));
        let users = SimpleUsers::build(&ids,key);
        assert!(users.index(id(2,1).as_bytes()).is_none());
        assert!(users.index(id(4,1).as_bytes()).is_none());
        assert!(users.index(id(5,1).as_bytes()).is_none());
        assert!(users.index(id(3,1).as_bytes()).is_none());
        assert_eq!(users.index(id(6,1).as_bytes()),Some(6));
        assert!(users.index(id(3,2).as_bytes()).is_none());
        for u in 0..7 { for a in 0..9 {
            let name = id(u,a);
            assert_eq!(users.resolve(name.as_bytes(),&ids), ids.get_compact_with_shard(name.as_bytes()));
            let upper = name.as_str().to_uppercase();
            assert_eq!(users.resolve(upper.as_bytes(),&ids),ids.get_compact_with_shard(upper.as_bytes()));
        }}
        assert!(users.index(b"u00001").is_none());
        assert!(users.index(b"uZZZZZZZZ").is_none());
    }
}

#[cfg(test)]
mod pipeline_tests {
    use super::*;
    use super::super::{FastPathKeys, run_fast_path_pipeline};
    use std::fs::{self, File};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use arrow_array::{StringArray, Int64Array};

    #[test]
    fn packed_pipeline_keeps_invalid_earliest_and_separate_aliases() {
        let temp = std::env::temp_dir().join(format!("simple-packed-winners-{}",std::process::id()));
        let data = temp.join("data");
        let output = temp.join("out");
        for dir in ["events", "clicks", "links"] { fs::create_dir_all(data.join(dir)).unwrap(); }
        let key = 12345;
        let id = |u: u64, a: u64| encoded_user((u << 9) | a, key).as_str().to_owned();
        let event = |n| { let mut b = encoded_user(n,key).as_bytes().to_vec(); b[0]=b'e'; String::from_utf8(b).unwrap() };
        fs::write(data.join("links/part.csv"), format!("a,b\n{},{}\n",id(1,0),id(1,1))).unwrap();
        fs::write(data.join("products.csv"), "product_id,category\np1,cat\n").unwrap();
        fs::write(data.join("campaigns.csv"), "campaign_id,channel\nc_1,channel\n").unwrap();
        fs::write(data.join("events/a.csv"), format!(
            "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n\
             {},{},bad,1970-01-01T00:00:00Z,view,p1,\n\
             {},{},0,1970-01-01T00:00:01Z,view,p1,\n\
             {},{},0,1970-01-01T00:00:00Z,view,p1,\n\
             {},{},1800000,1970-01-01T00:30:00Z,purchase,p1,100\n\
             {},{},3600001,1970-01-01T01:00:00Z,view,p1,\n\
             {},{},0,1970-01-01T00:00:00Z,view,p1,\n",
             event(0),id(1,0),event(0),id(1,3),event(1),id(1,0),
             event(2),id(1,1),event(3),id(1,0),event(4),id(1,3))).unwrap();
        // Same numeric ID, different exact spelling: both events survive.
        use std::io::Write;
        let mut events = fs::OpenOptions::new().append(true).open(data.join("events/a.csv")).unwrap();
        writeln!(events,"{},{},0,1970-01-01T00:00:00Z,view,p1,",event(1).to_uppercase(),id(1,3)).unwrap();
        drop(events);
        for workers in [1,4] {
            run_fast_path_pipeline(&data,&output,0,14*86_400_000,workers,
                FastPathKeys { user_key:key,event_key:key,click_key:key },std::time::Instant::now()).unwrap();
            let mut rows = Vec::new();
            for path in fs::read_dir(output.join("user_daily")).unwrap() {
                let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path.unwrap().path()).unwrap()).unwrap().build().unwrap();
                for batch in reader {
                    let batch=batch.unwrap();
                    let names=batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                    let sessions=batch.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
                    let events=batch.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
                    let revenue=batch.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
                    for i in 0..batch.num_rows() { rows.push((names.value(i).to_owned(),sessions.value(i),events.value(i),revenue.value(i))); }
                }
            }
            rows.sort();
            let mut expected=vec![(id(1,0).min(id(1,1)),2,3,100),(id(1,3),1,2,0)];
            expected.sort();
            assert_eq!(rows,expected);
            fs::remove_dir_all(&output).unwrap();
        }
        fs::remove_dir_all(temp).unwrap();
    }
}
