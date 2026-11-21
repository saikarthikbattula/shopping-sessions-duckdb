use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::hash::{BuildHasher, Hasher};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use arrow_array::builder::{Date32Builder, Int64Builder, StringBuilder};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::{InvalidIngest, Result};

#[cfg(target_os = "macos")]
const CLOCK_THREAD_CPUTIME_ID: i32 = 16;
#[cfg(not(target_os = "macos"))]
const CLOCK_THREAD_CPUTIME_ID: i32 = 3;

#[repr(C)]
struct Timespec {
    tv_sec: i64,
    tv_nsec: i64,
}

unsafe extern "C" {
    fn clock_gettime(clk_id: i32, tp: *mut Timespec) -> i32;
}

#[inline(always)]
fn thread_cpu_time_ns() -> u64 {
    let mut ts = Timespec { tv_sec: 0, tv_nsec: 0 };
    unsafe {
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &mut ts);
    }
    (ts.tv_sec as u64).wrapping_mul(1_000_000_000).wrapping_add(ts.tv_nsec as u64)
}

pub fn epoch_days(ts_ms: i64) -> i32 {
    ts_ms.div_euclid(86_400_000) as i32
}

pub fn user_daily_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]))
}

pub fn campaign_daily_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]))
}

#[derive(Clone, Copy)]
pub struct UserDailyRow {
    pub user_id: CompactId,
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue_cents: i64,
    pub top_category: Option<u8>,
}

pub struct CampaignDailyRow {
    pub campaign_id: String,
    pub channel: String,
    pub day: i32,
    pub sessions: i64,
    pub purchasing_sessions: i64,
    pub revenue_cents: i64,
    pub assisted_sessions: i64,
}


#[derive(Default, Clone, Copy)]
struct CampaignDailyAccumulator {
    sessions: i64,
    purchasing_sessions: i64,
    revenue_cents: i64,
    assisted_sessions: i64,
}

#[derive(Clone, Copy)]
pub struct CompactId {
    pub len: u8,
    pub bytes: [u8; 31],
}

impl CompactId {
    #[inline(always)]
    pub fn from_bytes(b: &[u8]) -> Self {
        if b.len() <= 31 {
            let mut bytes = [0u8; 31];
            bytes[..b.len()].copy_from_slice(b);
            CompactId {
                len: b.len() as u8,
                bytes,
            }
        } else {
            let leaked: &'static [u8] = Box::leak(b.to_vec().into_boxed_slice());
            let ptr = leaked.as_ptr() as usize;
            let full_len = leaked.len();
            let mut bytes = [0u8; 31];
            bytes[0..8].copy_from_slice(&ptr.to_ne_bytes());
            bytes[8..16].copy_from_slice(&full_len.to_ne_bytes());
            CompactId {
                len: 0xFF,
                bytes,
            }
        }
    }

    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        if self.len != 0xFF {
            &self.bytes[..self.len as usize]
        } else {
            let ptr = usize::from_ne_bytes(self.bytes[0..8].try_into().unwrap()) as *const u8;
            let len = usize::from_ne_bytes(self.bytes[8..16].try_into().unwrap());
            unsafe { std::slice::from_raw_parts(ptr, len) }
        }
    }

    #[inline(always)]
    pub fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
    }
}

impl std::fmt::Debug for CompactId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CompactId({})", self.as_str())
    }
}

impl PartialEq for CompactId {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for CompactId {}

impl Ord for CompactId {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

impl PartialOrd for CompactId {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for CompactId {
    #[inline(always)]
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.as_bytes());
    }
}

impl Default for CompactId {
    #[inline(always)]
    fn default() -> Self {
        CompactId {
            len: 0,
            bytes: [0u8; 31],
        }
    }
}

struct RawEvent {
    eid_hash: u128,
    person: CompactId,
    // Usually signed millisecond offsets from window_start. For wide values,
    // ingest_offset indexes the parsing worker's exact timestamp pairs.
    ingest_offset: u32,
    ts_offset: i32,
    price_cents: i32,
    p_shard: u8,
    cat_idx: u8,
    flags: u8,
}

const PURCHASE: u8 = 1;
const VALID: u8 = 2;
const WIDE_TIMES: u8 = 4;
const NO_EVENT_TS: u8 = 8;

#[derive(Default)]
struct ParsedEvents {
    shards: Vec<Vec<RawEvent>>,
    wide_times: Vec<(i64, i64)>,
}

impl ParsedEvents {
    #[inline]
    fn encode_times(&mut self, start: i64, ingest: i64, ts: Option<i64>) -> (u32, i32, u8) {
        let offset = |value: i64| {
            value.checked_sub(start).and_then(|delta| i32::try_from(delta).ok())
        };
        let ingest_offset = offset(ingest);
        let ts_offset = ts.map_or(Some(0), offset);
        let missing = if ts.is_none() { NO_EVENT_TS } else { 0 };
        if let (Some(ingest_offset), Some(ts_offset)) = (ingest_offset, ts_offset) {
            (ingest_offset as u32, ts_offset, missing)
        } else {
            let index = u32::try_from(self.wide_times.len()).expect("too many wide timestamp pairs");
            self.wide_times.push((ingest, ts.unwrap_or(i64::MIN)));
            (index, 0, WIDE_TIMES | missing)
        }
    }

    #[inline]
    fn decode_times(&self, event: &RawEvent, start: i64) -> (i64, i64) {
        if event.flags & WIDE_TIMES != 0 {
            self.wide_times[event.ingest_offset as usize]
        } else {
            let ingest = start + i64::from(event.ingest_offset as i32);
            let ts = if event.flags & NO_EVENT_TS != 0 {
                i64::MIN
            } else {
                start + i64::from(event.ts_offset)
            };
            (ingest, ts)
        }
    }
}

struct RawClick {
    click_id: CompactId,
    person: CompactId,
    ts_ms: i64,
    camp_idx: u16,
    p_shard: u8,
}

struct EventDedupEntry {
    person: CompactId,
    ingest_ms: i64,
    ts_ms: i64,
    price_cents: i32,
    p_shard: u8,
    cat_idx: u8,
    is_purchase: bool,
    is_valid: bool,
}

#[derive(Clone)]
struct RawPersonEvent {
    person: CompactId,
    ts_ms: i64,
    price_cents: i32,
    cat_idx: u8,
    is_purchase: bool,
}

#[derive(Clone)]
struct RawPersonClick {
    person: CompactId,
    ts_ms: i64,
    camp_idx: u16,
    click_id: CompactId,
}

// Sixteen-byte local record for person/timestamp sorting.
#[derive(Clone, Copy)]
struct LocalEvent {
    ts_ms: i64,
    p_idx: u32,
    price_category: u32,
}

impl LocalEvent {
    // Only validated events reach this representation. Purchases have prices
    // 1..=10_000_000 (24 bits); non-purchases carry zero.
    fn new(event: &RawPersonEvent, p_idx: u32) -> Self {
        let price = if event.is_purchase { event.price_cents as u32 } else { 0 };
        debug_assert!(price <= 10_000_000);
        Self {
            ts_ms: event.ts_ms,
            p_idx,
            price_category: price | (u32::from(event.cat_idx) << 24),
        }
    }

    fn price(&self) -> u32 { self.price_category & 0x00ff_ffff }
    fn category(&self) -> usize { (self.price_category >> 24) as usize }
}

#[derive(Clone, Copy)]
struct LocalClick {
    ts_ms: i64,
    click_id: CompactId,
    p_idx: u32,
    camp_idx: u16,
}

struct FastHasher(u64);
impl Hasher for FastHasher {
    #[inline(always)]
    fn finish(&self) -> u64 {
        // Mix all input bits before selecting table buckets or person shards.
        let mut value = self.0;
        value ^= value >> 33;
        value = value.wrapping_mul(0xff51afd7ed558ccd);
        value ^= value >> 33;
        value = value.wrapping_mul(0xc4ceb9fe1a85ec53);
        value ^ (value >> 33)
    }
    #[inline(always)]
    fn write(&mut self, bytes: &[u8]) {
        // Read complete words to avoid a dependent multiply for every ID byte.
        // HashMap and HashSet still compare full keys when hashes collide.
        let mut chunks = bytes.chunks_exact(8);
        for chunk in &mut chunks {
            let word = u64::from_le_bytes(chunk.try_into().unwrap());
            self.0 = (self.0 ^ word).rotate_left(27).wrapping_mul(0x3c79ac492ba7b653);
        }
        let remainder = chunks.remainder();
        if !remainder.is_empty() {
            let mut tail = [0u8; 8];
            tail[..remainder.len()].copy_from_slice(remainder);
            let word = u64::from_le_bytes(tail);
            self.0 = (self.0 ^ word).rotate_left(27).wrapping_mul(0x3c79ac492ba7b653);
        }
        self.0 ^= (bytes.len() as u64).wrapping_mul(0x1c69b3f74ac4ae35);
    }
    #[inline(always)]
    fn write_u128(&mut self, i: u128) {
        let lo = i as u64;
        let hi = (i >> 64) as u64;
        self.0 = (self.0.wrapping_mul(1099511628211) ^ lo).wrapping_mul(1099511628211) ^ hi;
    }
}

#[derive(Clone, Default)]
struct FastBuildHasher;
impl BuildHasher for FastBuildHasher {
    type Hasher = FastHasher;
    #[inline(always)]
    fn build_hasher(&self) -> FastHasher {
        FastHasher(14695981039346656037)
    }
}

const NUM_SHARDS: usize = 256;

#[inline(always)]
fn get_shard_bytes(key: &[u8]) -> usize {
    let mut hasher = FastBuildHasher.build_hasher();
    hasher.write(key);
    (hasher.finish() as usize) % NUM_SHARDS
}

#[inline(always)]
fn hash_event_id_128(bytes: &[u8]) -> (u128, usize) {
    // Word-at-a-time double-FNV: same multipliers as before but process 8 bytes
    // per iteration to cut loop overhead ~8x. Event IDs are fixed-length UUIDs
    // (36 bytes) so the tail is at most 4 bytes. HashMap still compares full
    // keys on collision; changing this hash only affects bucket distribution.
    const M1: u64 = 1099511628211;
    const M2: u64 = 14695981039346656037;
    let mut h1 = M2;
    let mut h2 = M1;
    let mut chunks = bytes.chunks_exact(8);
    for chunk in &mut chunks {
        let word = u64::from_le_bytes(chunk.try_into().unwrap());
        h1 = h1.wrapping_mul(M1) ^ word;
        h2 = h2.wrapping_mul(M2) ^ word;
    }
    let remainder = chunks.remainder();
    if !remainder.is_empty() {
        let mut tail = [0u8; 8];
        tail[..remainder.len()].copy_from_slice(remainder);
        let word = u64::from_le_bytes(tail);
        h1 = h1.wrapping_mul(M1) ^ word;
        h2 = h2.wrapping_mul(M2) ^ word;
    }
    // Length mixing prevents collisions between differently-padded inputs.
    h1 ^= (bytes.len() as u64).wrapping_mul(0x1c69b3f74ac4ae35);
    h2 ^= (bytes.len() as u64).wrapping_mul(0x517cc1b727220a95);
    let shard = (h1 as usize) % NUM_SHARDS;
    (((h1 as u128) << 64) | (h2 as u128), shard)
}

struct ResolvedPerson {
    // Frozen once per linked ID, then copied directly into parsed rows.
    id: CompactId,
    shard: u8,
}

pub struct FrozenIdentity {
    shards: Vec<HashMap<Box<[u8]>, ResolvedPerson, FastBuildHasher>>,
    unique_count: usize,
}

impl FrozenIdentity {
    pub fn empty() -> Self {
        Self {
            shards: (0..NUM_SHARDS).map(|_| HashMap::with_hasher(FastBuildHasher)).collect(),
            unique_count: 0,
        }
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn get<'a>(&'a self, user: &'a [u8]) -> &'a [u8] {
        if self.unique_count == 0 {
            return user;
        }
        let shard = get_shard_bytes(user);
        match self.shards[shard].get(user) {
            Some(canon) => canon.id.as_bytes(),
            None => user,
        }
    }

    #[inline(always)]
    fn get_compact_with_shard(&self, user: &[u8]) -> (CompactId, u8) {
        let shard = get_shard_bytes(user);
        if self.unique_count != 0 {
            if let Some(person) = self.shards[shard].get(user) {
                return (person.id, person.shard);
            }
        }
        (CompactId::from_bytes(user), shard as u8)
    }

    pub fn len(&self) -> usize {
        self.unique_count
    }
}

fn load_parallel_identity(links_dir: &Path, workers_count: usize) -> Result<FrozenIdentity> {
    if !links_dir.exists() {
        return Ok(FrozenIdentity::empty());
    }
    let files = list_csv_files(links_dir)?;
    if files.is_empty() {
        return Ok(FrozenIdentity::empty());
    }

    let file_batches: Vec<Vec<PathBuf>> = {
        let mut batches: Vec<Vec<PathBuf>> = (0..workers_count).map(|_| Vec::new()).collect();
        for (i, f) in files.into_iter().enumerate() {
            batches[i % workers_count].push(f);
        }
        batches
    };

    let worker_results: Vec<(Vec<Vec<u8>>, Vec<(usize, usize, usize, usize, usize)>)> = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for batch in file_batches {
            handles.push(scope.spawn(move || -> Result<(Vec<Vec<u8>>, Vec<(usize, usize, usize, usize, usize)>)> {
                let mut buffers = Vec::new();
                let mut edges = Vec::new(); // (buf_idx, a_start, a_len, b_start, b_len)
                for file_path in batch {
                    let bytes = fs::read(&file_path)?;
                    let buf_idx = buffers.len();
                    let mut fields = [None; 2];
                    for line in bytes.split(|&b| b == b'\n') {
                        if parse_csv_line(line, &mut fields) {
                            if let (Some(a), Some(b)) = (fields[0], fields[1]) {
                                if !a.is_empty() && !b.is_empty() {
                                    let a_offset = a.as_ptr() as usize - bytes.as_ptr() as usize;
                                    let b_offset = b.as_ptr() as usize - bytes.as_ptr() as usize;
                                    edges.push((buf_idx, a_offset, a.len(), b_offset, b.len()));
                                }
                            }
                        }
                    }
                    buffers.push(bytes);
                }
                Ok((buffers, edges))
            }));
        }
        let mut results = Vec::new();
        for h in handles {
            results.push(h.join().unwrap()?);
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(results)
    })?;

    let total_edges_est: usize = worker_results.iter().map(|(_, e)| e.len()).sum();
    let mut id_map: HashMap<&[u8], u32, FastBuildHasher> =
        HashMap::with_capacity_and_hasher(total_edges_est * 2, FastBuildHasher);
    let mut parents: Vec<u32> = Vec::with_capacity(total_edges_est * 2);
    let mut ranks: Vec<u8> = Vec::with_capacity(total_edges_est * 2);
    let mut minima: Vec<u32> = Vec::with_capacity(total_edges_est * 2);
    let mut names: Vec<&[u8]> = Vec::with_capacity(total_edges_est * 2);

    #[inline(always)]
    fn intern_id<'a>(
        slice: &'a [u8],
        id_map: &mut HashMap<&'a [u8], u32, FastBuildHasher>,
        parents: &mut Vec<u32>,
        ranks: &mut Vec<u8>,
        minima: &mut Vec<u32>,
        names: &mut Vec<&'a [u8]>,
    ) -> u32 {
        if let Some(&idx) = id_map.get(slice) {
            idx
        } else {
            let idx = names.len() as u32;
            id_map.insert(slice, idx);
            names.push(slice);
            parents.push(idx);
            ranks.push(0);
            minima.push(idx);
            idx
        }
    }

    fn find(mut i: u32, parents: &mut [u32]) -> u32 {
        let mut root = i;
        while parents[root as usize] != root {
            root = parents[root as usize];
        }
        while i != root {
            let next = parents[i as usize];
            parents[i as usize] = root;
            i = next;
        }
        root
    }

    for (buffers, edges) in &worker_results {
        for &(buf_idx, a_off, a_len, b_off, b_len) in edges {
            let a = &buffers[buf_idx][a_off..a_off + a_len];
            let b = &buffers[buf_idx][b_off..b_off + b_len];

            let u_a = intern_id(a, &mut id_map, &mut parents, &mut ranks, &mut minima, &mut names);
            let u_b = intern_id(b, &mut id_map, &mut parents, &mut ranks, &mut minima, &mut names);

            let root_a = find(u_a, &mut parents);
            let root_b = find(u_b, &mut parents);
            if root_a != root_b {
                let name_a = names[minima[root_a as usize] as usize];
                let name_b = names[minima[root_b as usize] as usize];
                let best_min = if name_a < name_b { minima[root_a as usize] } else { minima[root_b as usize] };
                if ranks[root_a as usize] < ranks[root_b as usize] {
                    parents[root_a as usize] = root_b;
                    minima[root_b as usize] = best_min;
                } else {
                    parents[root_b as usize] = root_a;
                    minima[root_a as usize] = best_min;
                    if ranks[root_a as usize] == ranks[root_b as usize] {
                        ranks[root_a as usize] += 1;
                    }
                }
            }
        }
    }

    let mut shards: Vec<HashMap<Box<[u8]>, ResolvedPerson, FastBuildHasher>> =
        (0..NUM_SHARDS).map(|_| HashMap::with_hasher(FastBuildHasher)).collect();

    let unique_count = names.len();
    for i in 0..names.len() {
        let root = find(i as u32, &mut parents);
        let best_name = names[minima[root as usize] as usize];
        let orig_name = names[i];
        if orig_name != best_name {
            let shard = get_shard_bytes(orig_name);
            let person = ResolvedPerson {
                id: CompactId::from_bytes(best_name),
                shard: get_shard_bytes(best_name) as u8,
            };
            shards[shard].insert(orig_name.to_vec().into_boxed_slice(), person);
        }
    }

    Ok(FrozenIdentity {
        shards,
        unique_count,
    })
}

#[inline(always)]
fn civil_to_days(y: i64, m: i64, d: i64) -> Option<i64> {
    if y == 0 || !(1..=12).contains(&m) {
        return None;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let mdays = [
        31,
        if leap { 29 } else { 28 },
        31, 30, 31, 30, 31, 31, 30, 31, 30, 31,
    ];
    if d < 1 || d > mdays[(m - 1) as usize] {
        return None;
    }
    let y_adj = y - if m <= 2 { 1 } else { 0 };
    let era = if y_adj >= 0 { y_adj } else { y_adj - 399 } / 400;
    let yoe = (y_adj - era * 400) as u32;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy as u32;
    Some(era * 146097 + doe as i64 - 719468)
}

thread_local! {
    static DATE_CACHE: std::cell::Cell<([u8; 10], i64)> = const { std::cell::Cell::new(([0u8; 10], 0)) };
}

#[inline(always)]
fn parse_f1_fast(b: &[u8]) -> Option<i64> {
    if b.len() != 20 {
        return None;
    }
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' || b[19] != b'Z' {
        return None;
    }
    let d = |i: usize| -> Option<i64> {
        let c = b[i];
        if c.is_ascii_digit() {
            Some((c - b'0') as i64)
        } else {
            None
        }
    };
    let h = d(11)? * 10 + d(12)?;
    let mi = d(14)? * 10 + d(15)?;
    let s = d(17)? * 10 + d(18)?;

    if h > 23 || mi > 59 || s > 59 {
        return None;
    }

    let prefix: [u8; 10] = b[0..10].try_into().unwrap();
    let days = DATE_CACHE.with(|cell| {
        let (cached_prefix, cached_days) = cell.get();
        if prefix == cached_prefix {
            Some(cached_days)
        } else {
            let y = d(0)? * 1000 + d(1)? * 100 + d(2)? * 10 + d(3)?;
            let m = d(5)? * 10 + d(6)?;
            let day = d(8)? * 10 + d(9)?;
            let computed = civil_to_days(y, m, day)?;
            cell.set((prefix, computed));
            Some(computed)
        }
    })?;

    Some((days * 86400 + h * 3600 + mi * 60 + s) * 1000)
}

#[inline(always)]
fn parse_ts_bytes(b: &[u8]) -> Option<i64> {
    if let Some(ms) = parse_f1_fast(b) {
        return Some(ms);
    }
    if b.is_empty() {
        return None;
    }
    if b.iter().all(u8::is_ascii_digit) {
        let mut n = 0i64;
        for &c in b {
            n = n.checked_mul(10)?.checked_add(i64::from(c - b'0'))?;
        }
        return Some(n);
    }
    if b.len() != 20 && b.len() != 25 {
        return None;
    }
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let number = |a: usize, z: usize| -> Option<i64> {
        let mut n = 0i64;
        for &v in &b[a..z] {
            if !v.is_ascii_digit() {
                return None;
            }
            n = n * 10 + i64::from(v - b'0');
        }
        Some(n)
    };
    let y = number(0, 4)?;
    let m = number(5, 7)?;
    let d = number(8, 10)?;
    let h = number(11, 13)?;
    let mi = number(14, 16)?;
    let se = number(17, 19)?;
    if h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let offset_min = if b.len() == 20 {
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
    let days = civil_to_days(y, m, d)?;
    Some((days * 86400 + h * 3600 + mi * 60 + se - offset_min * 60) * 1000)
}

#[inline(always)]
fn parse_window_ts_bytes(b: Option<&[u8]>, start: i64, end: i64) -> Option<i64> {
    let ts = parse_ts_bytes(b?)?;
    if ts >= start && ts < end {
        Some(ts)
    } else {
        None
    }
}

#[inline(always)]
fn parse_unsigned_bytes(b: Option<&[u8]>, maximum: i64) -> Option<i64> {
    let s = b?;
    if s.is_empty() {
        return None;
    }
    let mut n = 0i64;
    for &c in s {
        if !c.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add(i64::from(c - b'0'))?;
        if n > maximum {
            return None;
        }
    }
    Some(n)
}

#[inline(always)]
fn parse_csv_line<'a, const N: usize>(line: &'a [u8], fields: &mut [Option<&'a [u8]>; N]) -> bool {
    let line = if !line.is_empty() && line[line.len() - 1] == b'\n' {
        &line[..line.len() - 1]
    } else {
        line
    };
    if line.is_empty() {
        return false;
    }

    if memchr::memchr(b'"', line).is_none() {
        // Single AVX pass: collect all comma positions, then slice fields.
        // On Intel AVX-512 this scans the whole line in one vectorized pass
        // instead of N separate memchr calls.
        let len = line.len();
        let mut splits = [len; N]; // sentinel: last field ends at len
        let mut nsplits = 0usize;
        for pos in memchr::memchr_iter(b',', line) {
            if nsplits >= N {
                return false; // too many commas → wrong field count
            }
            splits[nsplits] = pos;
            nsplits += 1;
        }
        if nsplits + 1 != N {
            return false;
        }
        let mut prev = 0usize;
        for i in 0..N {
            let end = splits[i];
            let raw = &line[prev..end];
            fields[i] = if raw.is_empty() { None } else { Some(raw) };
            prev = end + 1;
        }
        return true;
    }

    let mut count = 0;
    let mut start = 0;
    let len = line.len();

    while start <= len {
        if count >= N {
            return false;
        }
        let end = match memchr::memchr(b',', &line[start..len]) {
            Some(pos) => start + pos,
            None => len,
        };
        let raw = &line[start..end];
        let val = if raw.is_empty() {
            None
        } else if raw[0] == b'"' {
            if raw.len() >= 2 && raw[raw.len() - 1] == b'"' {
                let inner = &raw[1..raw.len() - 1];
                if memchr::memchr(b'"', inner).is_some() {
                    return false;
                }
                if inner.is_empty() { None } else { Some(inner) }
            } else {
                return false;
            }
        } else {
            if memchr::memchr(b'"', raw).is_some() {
                return false;
            }
            Some(raw)
        };
        fields[count] = val;
        count += 1;
        if end == len {
            break;
        }
        start = end + 1;
    }

    count == N
}

fn list_csv_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if !dir.exists() {
        return Ok(files);
    }
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path
            .file_name()
            .is_some_and(|name| name.as_encoded_bytes().ends_with(b".csv"))
        {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

#[derive(Debug)]
struct Chunk {
    path: PathBuf,
    start: u64,
    end: u64,
}

fn plan_chunks(paths: &[PathBuf], size: u64) -> Result<Vec<Chunk>> {
    if paths.is_empty() {
        return Ok(Vec::new());
    }
    let chunks: Vec<Result<Vec<Chunk>>> = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for path in paths {
            handles.push(scope.spawn(move || -> Result<Vec<Chunk>> {
                let mut local = Vec::new();
                let file = File::open(path)?;
                let length = file.metadata()?.len();
                if length == 0 {
                    return Ok(local);
                }
                let mut reader = BufReader::new(file);
                reader.skip_until(b'\n')?;
                let mut start = reader.stream_position()?;
                while start < length {
                    let target = start.saturating_add(size);
                    let end = if target >= length {
                        length
                    } else {
                        reader.seek(SeekFrom::Start(target - 1))?;
                        reader.skip_until(b'\n')?;
                        reader.stream_position()?.min(length)
                    };
                    local.push(Chunk {
                        path: path.clone(),
                        start,
                        end,
                    });
                    start = end;
                }
                Ok(local)
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut result = Vec::new();
    for c in chunks {
        result.extend(c?);
    }
    Ok(result)
}

fn for_each_chunk_line(
    chunk: &Chunk,
    cancelled: &AtomicBool,
    mut consume: impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    let mut file = File::open(&chunk.path)?;
    file.seek(SeekFrom::Start(chunk.start))?;
    let mut reader = BufReader::with_capacity(512 * 1024, file);
    let mut position = chunk.start;
    let mut carry = Vec::new();

    while position < chunk.end && !cancelled.load(Ordering::Relaxed) {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            break;
        }
        let available = buf.len().min((chunk.end - position) as usize);
        let mut start = 0;

        while start < available {
            if let Some(pos) = memchr::memchr(b'\n', &buf[start..available]) {
                let end = start + pos;
                let line = &buf[start..=end];
                if carry.is_empty() {
                    consume(line)?;
                } else {
                    carry.extend_from_slice(line);
                    consume(&carry)?;
                    carry.clear();
                }
                start = end + 1;
            } else {
                carry.extend_from_slice(&buf[start..available]);
                break;
            }
        }
        reader.consume(available);
        position += available as u64;
    }
    if !carry.is_empty() {
        consume(&carry)?;
    }
    Ok(())
}

pub fn run_pipeline(
    data_dir: &Path,
    output_dir: &Path,
    start_opt: Option<i64>,
    end_opt: Option<i64>,
    workers_count: usize,
) -> Result<()> {
    let workers_count = workers_count.clamp(1, 16);

    let t_pipeline_start = std::time::Instant::now();

    // 1. Resolve window start and end
    let (window_start, window_end) = match (start_opt, end_opt) {
        (Some(s), Some(e)) => (s, e),
        _ => {
            let manifest_str = fs::read_to_string(data_dir.join("manifest.json"))?;
            let manifest: serde_json::Value = serde_json::from_str(&manifest_str)?;
            let s_str = manifest["window_start"]
                .as_str()
                .ok_or("manifest missing window_start")?;
            let e_str = manifest["window_end"]
                .as_str()
                .ok_or("manifest missing window_end")?;
            let s = parse_ts_bytes(s_str.as_bytes()).ok_or("invalid manifest window_start")?;
            let e = parse_ts_bytes(e_str.as_bytes()).ok_or("invalid manifest window_end")?;
            (s, e)
        }
    };
    if window_start >= window_end {
        return Err("window_start must be before window_end".into());
    }

    // 2. Identity resolution from links/
    let t_identity_start = std::time::Instant::now();
    let id_cpu_start = thread_cpu_time_ns();
    let ids = load_parallel_identity(&data_dir.join("links"), workers_count)?;
    let id_cpu_ns = thread_cpu_time_ns().saturating_sub(id_cpu_start);
    let t_identity_elapsed = t_identity_start.elapsed().as_secs_f64() * 1000.0;
    eprintln!("[identity] elapsed: {:.3}ms, unique identities: {}", t_identity_elapsed, ids.len());

    // 3. Products lookup: product_id -> category_idx
    let mut category_names = Vec::new();
    let mut category_to_idx: HashMap<String, u8> = HashMap::new();

    let get_or_insert_category = |name: &str, names: &mut Vec<String>, map: &mut HashMap<String, u8>| -> u8 {
        if let Some(&idx) = map.get(name) {
            idx
        } else {
            let idx = names.len() as u8;
            names.push(name.to_string());
            map.insert(name.to_string(), idx);
            idx
        }
    };

    let unknown_idx = get_or_insert_category("unknown", &mut category_names, &mut category_to_idx);

    let mut products_map: HashMap<Box<[u8]>, u8, FastBuildHasher> = HashMap::with_hasher(FastBuildHasher);
    let mut max_product_id: usize = 0;
    let mut parsed_products: Vec<(usize, u8)> = Vec::new();

    if data_dir.join("products.csv").exists() {
        let bytes = fs::read(data_dir.join("products.csv"))?;
        let mut fields = [None; 2];
        let mut first = true;
        for line in bytes.split(|&b| b == b'\n') {
            if first {
                first = false;
                continue;
            }
            if parse_csv_line(line, &mut fields) {
                if let (Some(pid), Some(cat)) = (fields[0], fields[1]) {
                    if let Ok(cat_str) = std::str::from_utf8(cat) {
                        let cat_idx = get_or_insert_category(cat_str, &mut category_names, &mut category_to_idx);
                        products_map.insert(pid.to_vec().into_boxed_slice(), cat_idx);
                        if pid.len() > 1 && pid[0] == b'p' && pid[1..].iter().all(u8::is_ascii_digit) {
                            if let Ok(s) = std::str::from_utf8(&pid[1..]) {
                                if let Ok(num) = s.parse::<usize>() {
                                    if num < 1_000_000 {
                                        parsed_products.push((num, cat_idx));
                                        if num > max_product_id {
                                            max_product_id = num;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let mut direct_products = vec![unknown_idx; max_product_id + 1];
    for (num, cat_idx) in parsed_products {
        direct_products[num] = cat_idx;
    }

    // 4. Campaigns lookup: campaign_id -> camp_idx
    let mut campaign_names = Vec::new();
    let mut campaign_channels = Vec::new();
    let mut campaign_to_idx: HashMap<CompactId, u16, FastBuildHasher> = HashMap::with_hasher(FastBuildHasher);

    if data_dir.join("campaigns.csv").exists() {
        let bytes = fs::read(data_dir.join("campaigns.csv"))?;
        let mut fields = [None; 2];
        let mut first = true;
        for line in bytes.split(|&b| b == b'\n') {
            if first {
                first = false;
                continue;
            }
            if parse_csv_line(line, &mut fields) {
                if let (Some(cid), Some(chan)) = (fields[0], fields[1]) {
                    let compact_cid = CompactId::from_bytes(cid);
                    let idx = campaign_names.len() as u16;
                    campaign_names.push(compact_cid);
                    campaign_channels.push(std::str::from_utf8(chan).unwrap_or("").to_string());
                    campaign_to_idx.insert(compact_cid, idx);
                }
            }
        }
    }

    // 5. Concurrent Overlapped Ingestion: Events and Clicks
    let (worker_person_events, worker_person_clicks, clicks_cpu_ns, events_parse_cpu_ns, events_dedup_cpu_ns, t_events_parse_elapsed, t_events_dedup_elapsed, t_clicks_elapsed, total_raw_events) = std::thread::scope(|scope| -> Result<(Vec<Vec<Vec<RawPersonEvent>>>, Vec<Vec<Vec<RawPersonClick>>>, u64, u64, u64, f64, f64, f64, usize)> {
        // Spawn clicks pipeline concurrently
        let click_handle = scope.spawn(|| -> Result<(Vec<Vec<Vec<RawPersonClick>>>, u64, f64)> {
            let t_clicks_start = std::time::Instant::now();
            let click_files = list_csv_files(&data_dir.join("clicks"))?;
            let click_chunks = Arc::new(plan_chunks(&click_files, 16 * 1024 * 1024)?);
            let next_chunk = Arc::new(AtomicUsize::new(0));
            let cancelled = Arc::new(AtomicBool::new(false));
            let click_workers_count = (workers_count / 2).clamp(1, 4);

            let (worker_click_shards, parse_cpu): (Vec<Vec<Vec<RawClick>>>, u64) = std::thread::scope(|c_scope| {
                let mut handles = Vec::new();
                for _ in 0..click_workers_count {
                    let click_chunks = Arc::clone(&click_chunks);
                    let next_chunk = Arc::clone(&next_chunk);
                    let cancelled = Arc::clone(&cancelled);
                    let campaign_to_idx = &campaign_to_idx;
                    let ids = &ids;

                    handles.push(c_scope.spawn(move || -> Result<(Vec<Vec<RawClick>>, u64)> {
                        let cpu_start = thread_cpu_time_ns();
                        let mut local_shards: Vec<Vec<RawClick>> = (0..NUM_SHARDS).map(|_| Vec::with_capacity(512)).collect();

                        while !cancelled.load(Ordering::Relaxed) {
                            let idx = next_chunk.fetch_add(1, Ordering::Relaxed);
                            let Some(chunk) = click_chunks.get(idx) else {
                                break;
                            };
                            for_each_chunk_line(chunk, &cancelled, |line| {
                                let mut fields = [None; 4];
                                if !parse_csv_line(line, &mut fields) {
                                    return Ok(());
                                }
                                let Some(click_id_bytes) = fields[0] else {
                                    return Ok(());
                                };
                                let ts_ms = parse_window_ts_bytes(
                                    fields[3],
                                    window_start - 604_800_000,
                                    window_end,
                                );
                                let camp_idx_opt = fields[2].and_then(|c| campaign_to_idx.get(&CompactId::from_bytes(c)).copied());
                                let is_valid = fields[1].is_some() && ts_ms.is_some() && camp_idx_opt.is_some();

                                if is_valid {
                                    let click_id = CompactId::from_bytes(click_id_bytes);
                                    let shard_idx = get_shard_bytes(click_id.as_bytes());
                                    let user_bytes = fields[1].unwrap();
                                    let (person, p_shard) = ids.get_compact_with_shard(user_bytes);
                                    local_shards[shard_idx].push(RawClick {
                                        click_id,
                                        person,
                                        camp_idx: camp_idx_opt.unwrap(),
                                        ts_ms: ts_ms.unwrap(),
                                        p_shard: p_shard as u8,
                                    });
                                }
                                Ok(())
                            })?;
                        }
                        let cpu_ns = thread_cpu_time_ns().saturating_sub(cpu_start);
                        Ok((local_shards, cpu_ns))
                    }));
                }
                let mut results = Vec::new();
                for h in handles {
                    results.push(h.join().unwrap()?);
                }
                let total_cpu: u64 = results.iter().map(|(_, c)| *c).sum();
                let shards: Vec<Vec<Vec<RawClick>>> = results.into_iter().map(|(s, _)| s).collect();
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>((shards, total_cpu))
            })?;

            // Deduplication of clicks
            let (worker_person_clicks, dedup_cpu): (Vec<Vec<Vec<RawPersonClick>>>, u64) = std::thread::scope(|c_scope| {
                let shards_per_worker = (NUM_SHARDS + click_workers_count - 1) / click_workers_count;
                let mut handles = Vec::new();
                let worker_click_shards = &worker_click_shards;

                for worker_idx in 0..click_workers_count {
                    let start_shard = worker_idx * shards_per_worker;
                    let end_shard = (start_shard + shards_per_worker).min(NUM_SHARDS);

                    handles.push(c_scope.spawn(move || -> (Vec<Vec<RawPersonClick>>, u64) {
                        let cpu_start = thread_cpu_time_ns();
                        let mut local_person_shards: Vec<Vec<RawPersonClick>> =
                            (0..NUM_SHARDS).map(|_| Vec::with_capacity(256)).collect();

                        let mut seen_clicks: HashSet<CompactId, FastBuildHasher> =
                            HashSet::with_capacity_and_hasher(16384, FastBuildHasher);

                        for shard_idx in start_shard..end_shard {
                            seen_clicks.clear();

                            for worker_shards in worker_click_shards {
                                for cl in &worker_shards[shard_idx] {
                                    if seen_clicks.insert(cl.click_id) {
                                        local_person_shards[cl.p_shard as usize].push(RawPersonClick {
                                            person: cl.person,
                                            ts_ms: cl.ts_ms,
                                            camp_idx: cl.camp_idx,
                                            click_id: cl.click_id,
                                        });
                                    }
                                }
                            }
                        }
                        let cpu_ns = thread_cpu_time_ns().saturating_sub(cpu_start);
                        (local_person_shards, cpu_ns)
                    }));
                }
                let mut shards = Vec::new();
                let mut total_cpu = 0u64;
                for h in handles {
                    let (s, c) = h.join().unwrap();
                    shards.push(s);
                    total_cpu += c;
                }
                (shards, total_cpu)
            });
            let clicks_cpu_ns = parse_cpu + dedup_cpu;
            let t_clicks_elapsed = t_clicks_start.elapsed().as_secs_f64() * 1000.0;
            eprintln!("[clicks_total] wall: {:.3}ms, cpu: {:.3}ms", t_clicks_elapsed, (clicks_cpu_ns as f64) / 1_000_000.0);
            Ok((worker_person_clicks, clicks_cpu_ns, t_clicks_elapsed))
        });

        // Event Parsing
        let t_events_parse_start = std::time::Instant::now();
        let event_files = list_csv_files(&data_dir.join("events"))?;
        let event_chunks = Arc::new(plan_chunks(&event_files, 16 * 1024 * 1024)?);
        let next_chunk = Arc::new(AtomicUsize::new(0));
        let cancelled = Arc::new(AtomicBool::new(false));

        let (worker_event_shards, events_parse_cpu_ns): (Vec<ParsedEvents>, u64) = std::thread::scope(|e_scope| {
            let mut handles = Vec::new();
            for _ in 0..workers_count {
                let event_chunks = Arc::clone(&event_chunks);
                let next_chunk = Arc::clone(&next_chunk);
                let cancelled = Arc::clone(&cancelled);
                let products_map = &products_map;
                let direct_products = &direct_products;
                let ids = &ids;

                handles.push(e_scope.spawn(move || -> Result<(ParsedEvents, u64)> {
                    let cpu_start = thread_cpu_time_ns();
                    let mut parsed = ParsedEvents {
                        shards: (0..NUM_SHARDS).map(|_| Vec::with_capacity(4096)).collect(),
                        wide_times: Vec::new(),
                    };

                    while !cancelled.load(Ordering::Relaxed) {
                        let idx = next_chunk.fetch_add(1, Ordering::Relaxed);
                        let Some(chunk) = event_chunks.get(idx) else {
                            break;
                        };
                        for_each_chunk_line(chunk, &cancelled, |line| {
                            let mut fields = [None; 7];
                            if !parse_csv_line(line, &mut fields) {
                                return Ok(());
                            }
                            let Some(event_id_bytes) = fields[0] else {
                                return Ok(());
                            };
                            let ingest_ms = parse_ts_bytes(fields[3].unwrap_or(&[])).ok_or(InvalidIngest)?;
                            let ts_ms_opt = parse_window_ts_bytes(fields[2], window_start, window_end);
                            // Classify event type once; both validity and purchase flag use it.
                            let event_type = fields[4];
                            let is_purchase = event_type == Some(b"purchase");
                            let valid_event_type = matches!(event_type, Some(b"view" | b"cart" | b"purchase"));
                            let price_cents = if is_purchase {
                                parse_unsigned_bytes(fields[6], 10000000).filter(|v| *v > 0)
                            } else {
                                None
                            };
                            let is_valid = fields[1].is_some()
                                && ts_ms_opt.is_some()
                                && fields[5].is_some()
                                && valid_event_type
                                && (!is_purchase || price_cents.is_some());

                            // Product-category lookup: only consult the HashMap for valid events
                            // (or events that have a product id). Invalid-event-type rows need
                            // cat_idx = unknown_idx in RawEvent; skip the hashmap probe for them.
                            let category_idx = if is_valid {
                                // Only valid events have fields[5].is_some() guaranteed here.
                                let pid = fields[5].unwrap();
                                if pid.len() > 1 && pid[0] == b'p' {
                                    let mut num: usize = 0;
                                    let mut valid_num = true;
                                    for &c in &pid[1..] {
                                        if c.is_ascii_digit() {
                                            num = num * 10 + (c - b'0') as usize;
                                        } else {
                                            valid_num = false;
                                            break;
                                        }
                                    }
                                    if valid_num && num < direct_products.len() {
                                        let cat = direct_products[num];
                                        if cat != unknown_idx {
                                            cat
                                        } else {
                                            products_map.get(pid).copied().unwrap_or(unknown_idx)
                                        }
                                    } else {
                                        products_map.get(pid).copied().unwrap_or(unknown_idx)
                                    }
                                } else {
                                    products_map.get(pid).copied().unwrap_or(unknown_idx)
                                }
                            } else if let Some(pid) = fields[5] {
                                // Event type is invalid but product id exists: still look up
                                // category so the RawEvent carries the right cat_idx (used by
                                // analytics even for some non-valid paths). Avoid the HashMap
                                // probe for the common pNNN fast path.
                                if pid.len() > 1 && pid[0] == b'p' {
                                    let mut num: usize = 0;
                                    let mut valid_num = true;
                                    for &c in &pid[1..] {
                                        if c.is_ascii_digit() {
                                            num = num * 10 + (c - b'0') as usize;
                                        } else {
                                            valid_num = false;
                                            break;
                                        }
                                    }
                                    if valid_num && num < direct_products.len() {
                                        direct_products[num]
                                        // Skip the HashMap fallback for invalid events;
                                        // unknown_idx is fine since they won't affect results.
                                    } else {
                                        unknown_idx
                                    }
                                } else {
                                    unknown_idx
                                }
                            } else {
                                unknown_idx
                            };

                            let (eid_hash, shard_idx) = hash_event_id_128(event_id_bytes);
                            let (person, p_shard) = if let Some(user_bytes) = fields[1] {
                                ids.get_compact_with_shard(user_bytes)
                            } else {
                                (CompactId::default(), 0u8)
                            };

                            let (ingest_offset, ts_offset, time_flags) =
                                parsed.encode_times(window_start, ingest_ms, ts_ms_opt);
                            let flags = time_flags
                                | (if is_purchase { PURCHASE } else { 0 })
                                | (if is_valid { VALID } else { 0 });
                            parsed.shards[shard_idx].push(RawEvent {
                                eid_hash,
                                person,
                                ingest_offset,
                                ts_offset,
                                price_cents: price_cents.unwrap_or(0) as i32,
                                p_shard,
                                cat_idx: category_idx,
                                flags,
                            });
                            Ok(())
                        })?;
                    }
                    let cpu_ns = thread_cpu_time_ns().saturating_sub(cpu_start);
                    Ok((parsed, cpu_ns))
                }));
            }
            let mut results = Vec::new();
            for h in handles {
                results.push(h.join().unwrap()?);
            }
            let total_cpu: u64 = results.iter().map(|(_, c)| *c).sum();
            let shards = results.into_iter().map(|(s, _)| s).collect();
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((shards, total_cpu))
        })?;
        let t_events_parse_elapsed = t_events_parse_start.elapsed().as_secs_f64() * 1000.0;
        eprintln!("[events_parse] wall: {:.3}ms, cpu: {:.3}ms", t_events_parse_elapsed, (events_parse_cpu_ns as f64) / 1_000_000.0);

        let total_raw_events: usize = worker_event_shards.iter().flat_map(|w| &w.shards).map(Vec::len).sum();

        // 6. Lock-Free Parallel Deduplication of Events and Routing to Person Shards
        let t_events_dedup_start = std::time::Instant::now();
        let (worker_person_events, events_dedup_cpu_ns): (Vec<Vec<Vec<RawPersonEvent>>>, u64) = std::thread::scope(|e_scope| {
            let shards_per_worker = (NUM_SHARDS + workers_count - 1) / workers_count;
            let mut handles = Vec::new();
            let worker_event_shards = &worker_event_shards;

            for worker_idx in 0..workers_count {
                let start_shard = worker_idx * shards_per_worker;
                let end_shard = (start_shard + shards_per_worker).min(NUM_SHARDS);

                handles.push(e_scope.spawn(move || -> (Vec<Vec<RawPersonEvent>>, u64) {
                    let cpu_start = thread_cpu_time_ns();
                    let mut local_person_shards: Vec<Vec<RawPersonEvent>> =
                        (0..NUM_SHARDS).map(|_| Vec::with_capacity(1024)).collect();

                    let mut dedup_map: HashMap<u128, EventDedupEntry, FastBuildHasher> =
                        HashMap::with_capacity_and_hasher(65536, FastBuildHasher);

                    for shard_idx in start_shard..end_shard {
                        dedup_map.clear();

                        for worker_shards in worker_event_shards {
                            for ev in &worker_shards.shards[shard_idx] {
                                let (ingest_ms, ts_ms) = worker_shards.decode_times(ev, window_start);
                                match dedup_map.get_mut(&ev.eid_hash) {
                                    Some(existing) => {
                                        if ingest_ms < existing.ingest_ms {
                                            *existing = EventDedupEntry {
                                                person: ev.person,
                                                ingest_ms,
                                                ts_ms,
                                                price_cents: ev.price_cents,
                                                p_shard: ev.p_shard,
                                                cat_idx: ev.cat_idx,
                                                is_purchase: ev.flags & PURCHASE != 0,
                                                is_valid: ev.flags & VALID != 0,
                                            };
                                        }
                                    }
                                    None => {
                                        dedup_map.insert(
                                            ev.eid_hash,
                                            EventDedupEntry {
                                                person: ev.person,
                                                ingest_ms,
                                                ts_ms,
                                                price_cents: ev.price_cents,
                                                p_shard: ev.p_shard,
                                                cat_idx: ev.cat_idx,
                                                is_purchase: ev.flags & PURCHASE != 0,
                                                is_valid: ev.flags & VALID != 0,
                                            },
                                        );
                                    }
                                }
                            }
                        }

                        for entry in dedup_map.values() {
                            if entry.is_valid && entry.ts_ms != i64::MIN {
                                let ts = entry.ts_ms;
                                if entry.ingest_ms.saturating_sub(ts) <= 86_400_000 {
                                    local_person_shards[entry.p_shard as usize].push(RawPersonEvent {
                                        person: entry.person,
                                        ts_ms: ts,
                                        price_cents: entry.price_cents,
                                        cat_idx: entry.cat_idx,
                                        is_purchase: entry.is_purchase,
                                    });
                                }
                            }
                        }
                    }
                    let cpu_ns = thread_cpu_time_ns().saturating_sub(cpu_start);
                    (local_person_shards, cpu_ns)
                }));
            }
            let mut shards = Vec::new();
            let mut total_cpu = 0u64;
            for h in handles {
                let (s, c) = h.join().unwrap();
                shards.push(s);
                total_cpu += c;
            }
            (shards, total_cpu)
        });
        let t_events_dedup_elapsed = t_events_dedup_start.elapsed().as_secs_f64() * 1000.0;
        eprintln!("[events_dedup] wall: {:.3}ms, cpu: {:.3}ms", t_events_dedup_elapsed, (events_dedup_cpu_ns as f64) / 1_000_000.0);

        let (worker_person_clicks, clicks_cpu_ns, t_clicks_elapsed) = click_handle.join().unwrap()?;
        Ok((worker_person_events, worker_person_clicks, clicks_cpu_ns, events_parse_cpu_ns, events_dedup_cpu_ns, t_events_parse_elapsed, t_events_dedup_elapsed, t_clicks_elapsed, total_raw_events))
    })?;

    // 9. Parallel Execution of Analytics across Person Shards using 16-byte integer-keyed local events
    let t_analytics_start = std::time::Instant::now();
    let out_user_dir = output_dir.join("user_daily");
    fs::create_dir_all(&out_user_dir)?;

    let partial_results: Vec<Result<(HashMap<(u16, i32), CampaignDailyAccumulator, FastBuildHasher>, u64)>> =
        std::thread::scope(|scope| {
            let shards_per_worker = (NUM_SHARDS + workers_count - 1) / workers_count;
            let mut handles = Vec::new();
            let worker_person_events = &worker_person_events;
            let worker_person_clicks = &worker_person_clicks;
            let category_names = &category_names;
            let campaign_count = campaign_names.len();

            for worker_idx in 0..workers_count {
                let start_shard = worker_idx * shards_per_worker;
                let end_shard = (start_shard + shards_per_worker).min(NUM_SHARDS);

                handles.push(scope.spawn(move || -> Result<(HashMap<(u16, i32), CampaignDailyAccumulator, FastBuildHasher>, u64)> {
                    let cpu_start = thread_cpu_time_ns();
                    let mut user_rows = Vec::new();
                    let profiling = std::env::var_os("NATIVE_PROFILE").as_deref() == Some(std::ffi::OsStr::new("1"));
                    let worker_started = profiling.then(std::time::Instant::now);
                    let mut sort_time = std::time::Duration::ZERO;
                    let mut attribution_time = std::time::Duration::ZERO;

                    let min_day = epoch_days(window_start);
                    let max_day = epoch_days(window_end);
                    let num_days = (max_day - min_day + 1).max(1) as usize;
                    let total_slots = (campaign_count + 1) * num_days;
                    let use_dense = total_slots <= 131_072 && campaign_count > 0;

                    let mut dense_table = if use_dense {
                        vec![CampaignDailyAccumulator::default(); total_slots]
                    } else {
                        Vec::new()
                    };
                    let mut sparse_map: HashMap<(u16, i32), CampaignDailyAccumulator, FastBuildHasher> =
                        HashMap::with_hasher(FastBuildHasher);

                    let mut shard_person_map: HashMap<CompactId, u32, FastBuildHasher> =
                        HashMap::with_capacity_and_hasher(16384, FastBuildHasher);
                    let mut shard_person_names: Vec<CompactId> = Vec::with_capacity(16384);
                    let mut local_events: Vec<LocalEvent> = Vec::with_capacity(65536);
                    let mut local_clicks: Vec<LocalClick> = Vec::with_capacity(16384);

                    for shard_idx in start_shard..end_shard {
                        shard_person_map.clear();
                        shard_person_names.clear();
                        local_events.clear();
                        local_clicks.clear();

                        let mut cache_person = [CompactId::default(); 16];
                        let mut cache_pidx = [0u32; 16];
                        let mut cache_valid = [false; 16];

                        for w_events in worker_person_events {
                            for ev in &w_events[shard_idx] {
                                let slot = (ev.person.bytes[0] as usize ^ ev.person.bytes[7] as usize) & 15;
                                let p_idx = if cache_valid[slot] && cache_person[slot] == ev.person {
                                    cache_pidx[slot]
                                } else {
                                    let idx = match shard_person_map.get(&ev.person) {
                                        Some(&idx) => idx,
                                        None => {
                                            let idx = shard_person_names.len() as u32;
                                            shard_person_names.push(ev.person);
                                            shard_person_map.insert(ev.person, idx);
                                            idx
                                        }
                                    };
                                    cache_person[slot] = ev.person;
                                    cache_pidx[slot] = idx;
                                    cache_valid[slot] = true;
                                    idx
                                };
                                local_events.push(LocalEvent::new(ev, p_idx));
                            }
                        }

                        for w_clicks in worker_person_clicks {
                            for cl in &w_clicks[shard_idx] {
                                let slot = (cl.person.bytes[0] as usize ^ cl.person.bytes[7] as usize) & 15;
                                let p_opt = if cache_valid[slot] && cache_person[slot] == cl.person {
                                    Some(cache_pidx[slot])
                                } else {
                                    let opt = shard_person_map.get(&cl.person).copied();
                                    if let Some(idx) = opt {
                                        cache_person[slot] = cl.person;
                                        cache_pidx[slot] = idx;
                                        cache_valid[slot] = true;
                                    }
                                    opt
                                };
                                if let Some(p_idx) = p_opt {
                                    local_clicks.push(LocalClick {
                                        ts_ms: cl.ts_ms,
                                        click_id: cl.click_id,
                                        p_idx,
                                        camp_idx: cl.camp_idx,
                                    });
                                }
                            }
                        }

                        let sort_started = profiling.then(std::time::Instant::now);
                        if window_end.abs_diff(window_start) <= u64::from(u32::MAX) + 1 {
                            local_events.sort_unstable_by_key(|e| {
                                (u64::from(e.p_idx) << 32) | (e.ts_ms - window_start) as u64
                            });
                        } else {
                            local_events.sort_unstable_by_key(|e| (e.p_idx, e.ts_ms));
                        }
                        local_clicks.sort_unstable_by(|a, b| {
                            (a.p_idx, a.ts_ms, a.click_id.as_bytes()).cmp(&(b.p_idx, b.ts_ms, b.click_id.as_bytes()))
                        });
                        if let Some(started) = sort_started { sort_time += started.elapsed(); }

                        let attribution_started = profiling.then(std::time::Instant::now);
                        let mut sink = if use_dense {
                            CampaignDailySink::Dense {
                                table: &mut dense_table,
                                min_day,
                                num_days,
                                campaign_count,
                            }
                        } else {
                            CampaignDailySink::Sparse {
                                map: &mut sparse_map,
                            }
                        };
                        let u_part = execute_shard_local(
                            &local_events,
                            &local_clicks,
                            &shard_person_names,
                            category_names,
                            &mut sink,
                        );
                        user_rows.extend(u_part);
                        if let Some(started) = attribution_started { attribution_time += started.elapsed(); }
                    }

                    let campaign_daily_map = if use_dense {
                        let mut map = HashMap::with_capacity_and_hasher(4096, FastBuildHasher);
                        for c_idx in 0..=campaign_count {
                            let camp_key = if c_idx == campaign_count { u16::MAX } else { c_idx as u16 };
                            let base = c_idx * num_days;
                            for d_off in 0..num_days {
                                let acc = dense_table[base + d_off];
                                if acc.sessions > 0 || acc.assisted_sessions > 0 {
                                    let day = min_day + d_off as i32;
                                    map.insert((camp_key, day), acc);
                                }
                            }
                        }
                        map
                    } else {
                        sparse_map
                    };

                    let parquet_started = profiling.then(std::time::Instant::now);
                    if worker_idx == 0 || !user_rows.is_empty() {
                        write_user_daily_shard(output_dir, worker_idx, &user_rows, category_names)?;
                    }
                    if let Some(started) = worker_started {
                        eprintln!("[profile/worker] worker={worker_idx} wall_ms={:.3} sort_ms={:.3} attribution_ms={:.3} parquet_ms={:.3} rows={} raw_event_bytes={} person_event_bytes={} local_event_bytes={}",
                            started.elapsed().as_secs_f64()*1000.0, sort_time.as_secs_f64()*1000.0,
                            attribution_time.as_secs_f64()*1000.0, parquet_started.unwrap().elapsed().as_secs_f64()*1000.0,
                            user_rows.len(), std::mem::size_of::<RawEvent>(),
                            std::mem::size_of::<RawPersonEvent>(), std::mem::size_of::<LocalEvent>());
                    }

                    let cpu_ns = thread_cpu_time_ns().saturating_sub(cpu_start);
                    Ok((campaign_daily_map, cpu_ns))
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

    let mut merged_campaign_map: HashMap<(u16, i32), CampaignDailyAccumulator, FastBuildHasher> =
        HashMap::with_hasher(FastBuildHasher);
    let mut analytics_cpu_ns = 0u64;

    for c_map_res in partial_results {
        let (c_map, cpu_ns) = c_map_res?;
        analytics_cpu_ns += cpu_ns;
        for (k, acc) in c_map {
            let entry = merged_campaign_map.entry(k).or_default();
            entry.sessions += acc.sessions;
            entry.purchasing_sessions += acc.purchasing_sessions;
            entry.revenue_cents += acc.revenue_cents;
            entry.assisted_sessions += acc.assisted_sessions;
        }
    }

    // Format CampaignDaily rows
    let organic_camp_idx = u16::MAX;
    let mut campaign_rows = Vec::with_capacity(merged_campaign_map.len());
    for ((camp_idx, day), acc) in merged_campaign_map {
        let (campaign_id, channel) = if camp_idx == organic_camp_idx {
            ("organic".to_string(), "organic".to_string())
        } else {
            let cid = campaign_names[camp_idx as usize].as_str().to_string();
            let chan = campaign_channels[camp_idx as usize].clone();
            (cid, chan)
        };
        campaign_rows.push(CampaignDailyRow {
            campaign_id,
            channel,
            day,
            sessions: acc.sessions,
            purchasing_sessions: acc.purchasing_sessions,
            revenue_cents: acc.revenue_cents,
            assisted_sessions: acc.assisted_sessions,
        });
    }
    let t_analytics_elapsed = t_analytics_start.elapsed().as_secs_f64() * 1000.0;
    eprintln!("[analytics_sessions_sort_parquet] wall: {:.3}ms, cpu: {:.3}ms", t_analytics_elapsed, (analytics_cpu_ns as f64) / 1_000_000.0);

    // 10. Write CampaignDaily Parquet File
    let t_parquet_start = std::time::Instant::now();
    let camp_cpu_start = thread_cpu_time_ns();
    write_campaign_daily(output_dir, campaign_rows)?;
    let camp_cpu_ns = thread_cpu_time_ns().saturating_sub(camp_cpu_start);
    let t_parquet_elapsed = t_parquet_start.elapsed().as_secs_f64() * 1000.0;
    eprintln!("[campaign_parquet_write] wall: {:.3}ms, cpu: {:.3}ms", t_parquet_elapsed, (camp_cpu_ns as f64) / 1_000_000.0);

    let total_cpu_ns = id_cpu_ns + clicks_cpu_ns + events_parse_cpu_ns + events_dedup_cpu_ns + analytics_cpu_ns + camp_cpu_ns;
    let cpu_per_event_us = (total_cpu_ns as f64) / (total_raw_events.max(1) as f64 * 1000.0);
    eprintln!("[timing_breakdown] total_events: {}, wall: {:.3}ms, cpu_sum: {:.3}ms ({:.3} µs/event)",
        total_raw_events,
        t_pipeline_start.elapsed().as_secs_f64() * 1000.0,
        (total_cpu_ns as f64) / 1_000_000.0,
        cpu_per_event_us
    );
    eprintln!("  identity:           wall: {:>8.3}ms, cpu: {:>8.3}ms ({:>5.1}%)", t_identity_elapsed, (id_cpu_ns as f64) / 1_000_000.0, (id_cpu_ns as f64) * 100.0 / total_cpu_ns.max(1) as f64);
    eprintln!("  events_parse:       wall: {:>8.3}ms, cpu: {:>8.3}ms ({:>5.1}%)", t_events_parse_elapsed, (events_parse_cpu_ns as f64) / 1_000_000.0, (events_parse_cpu_ns as f64) * 100.0 / total_cpu_ns.max(1) as f64);
    eprintln!("  events_dedup:       wall: {:>8.3}ms, cpu: {:>8.3}ms ({:>5.1}%)", t_events_dedup_elapsed, (events_dedup_cpu_ns as f64) / 1_000_000.0, (events_dedup_cpu_ns as f64) * 100.0 / total_cpu_ns.max(1) as f64);
    eprintln!("  clicks_total:       wall: {:>8.3}ms, cpu: {:>8.3}ms ({:>5.1}%)", t_clicks_elapsed, (clicks_cpu_ns as f64) / 1_000_000.0, (clicks_cpu_ns as f64) * 100.0 / total_cpu_ns.max(1) as f64);
    eprintln!("  analytics_sessions: wall: {:>8.3}ms, cpu: {:>8.3}ms ({:>5.1}%)", t_analytics_elapsed, (analytics_cpu_ns as f64) / 1_000_000.0, (analytics_cpu_ns as f64) * 100.0 / total_cpu_ns.max(1) as f64);
    eprintln!("  campaign_write:     wall: {:>8.3}ms, cpu: {:>8.3}ms ({:>5.1}%)", t_parquet_elapsed, (camp_cpu_ns as f64) / 1_000_000.0, (camp_cpu_ns as f64) * 100.0 / total_cpu_ns.max(1) as f64);

    Ok(())
}

enum CampaignDailySink<'a> {
    Dense {
        table: &'a mut [CampaignDailyAccumulator],
        min_day: i32,
        num_days: usize,
        campaign_count: usize,
    },
    Sparse {
        map: &'a mut HashMap<(u16, i32), CampaignDailyAccumulator, FastBuildHasher>,
    },
}

impl<'a> CampaignDailySink<'a> {
    #[inline(always)]
    fn record_credit(&mut self, camp_idx: u16, day: i32, purchases: i64, revenue: i64) {
        match self {
            Self::Dense { table, min_day, num_days, campaign_count } => {
                let c_idx = if camp_idx == u16::MAX { *campaign_count } else { camp_idx as usize };
                let d_off = (day - *min_day) as usize;
                let slot = c_idx * *num_days + d_off;
                let entry = &mut table[slot];
                entry.sessions += 1;
                if purchases > 0 {
                    entry.purchasing_sessions += 1;
                }
                entry.revenue_cents += revenue;
            }
            Self::Sparse { map } => {
                let entry = map.entry((camp_idx, day)).or_default();
                entry.sessions += 1;
                if purchases > 0 {
                    entry.purchasing_sessions += 1;
                }
                entry.revenue_cents += revenue;
            }
        }
    }

    #[inline(always)]
    fn record_assist(&mut self, camp_idx: u16, day: i32) {
        match self {
            Self::Dense { table, min_day, num_days, .. } => {
                let d_off = (day - *min_day) as usize;
                let slot = (camp_idx as usize) * *num_days + d_off;
                table[slot].assisted_sessions += 1;
            }
            Self::Sparse { map } => {
                map.entry((camp_idx, day)).or_default().assisted_sessions += 1;
            }
        }
    }
}

fn execute_shard_local(
    events: &[LocalEvent],
    clicks: &[LocalClick],
    person_names: &[CompactId],
    category_names: &[String],
    campaign_sink: &mut CampaignDailySink,
) -> Vec<UserDailyRow> {
    let mut user_rows = Vec::new();

    let mut ev_start = 0;
    let mut cl_cursor = 0;

    while ev_start < events.len() {
        let p_idx = events[ev_start].p_idx;
        let mut ev_end = ev_start + 1;
        while ev_end < events.len() && events[ev_end].p_idx == p_idx {
            ev_end += 1;
        }
        let person_events = &events[ev_start..ev_end];

        // Advance click cursor to this person's p_idx
        while cl_cursor < clicks.len() && clicks[cl_cursor].p_idx < p_idx {
            cl_cursor += 1;
        }
        let cl_start = cl_cursor;
        while cl_cursor < clicks.len() && clicks[cl_cursor].p_idx == p_idx {
            cl_cursor += 1;
        }
        let person_clicks = &clicks[cl_start..cl_cursor];

        // Process sessions for this person with zero-allocation scalar day aggregation
        let person = person_names[p_idx as usize];
        let mut curr_day: i32 = -1;
        let mut day_sessions: i64 = 0;
        let mut day_events: i64 = 0;
        let mut day_revenue: i64 = 0;
        let mut day_has_purchases = false;
        let mut cat_rev = [0i64; 256];

        let mut session_start_idx = 0;
        let mut prev_ts = person_events[0].ts_ms;
        let mut click_left = 0usize;
        let mut click_right = 0usize;

        let flush_day = |day: i32,
                         sessions: i64,
                         events: i64,
                         revenue: i64,
                         has_purchases: bool,
                         cat_rev: &[i64; 256],
                         category_names: &[String],
                         person: CompactId,
                         user_rows: &mut Vec<UserDailyRow>| {
            let top_category = if !has_purchases {
                None
            } else {
                let mut best_cat: Option<u8> = None;
                let mut best_rev: i64 = 0;
                for cat in 0..category_names.len() as u8 {
                    let rev = cat_rev[cat as usize];
                    if rev > 0 {
                        match best_cat {
                            None => {
                                best_cat = Some(cat);
                                best_rev = rev;
                            }
                            Some(prev) => {
                                if rev > best_rev
                                    || (rev == best_rev
                                        && category_names[cat as usize].as_bytes()
                                            < category_names[prev as usize].as_bytes())
                                {
                                    best_cat = Some(cat);
                                    best_rev = rev;
                                }
                            }
                        }
                    }
                }
                best_cat
            };
            user_rows.push(UserDailyRow {
                user_id: person,
                day,
                sessions,
                events,
                revenue_cents: revenue,
                top_category,
            });
        };

        for i in 1..person_events.len() {
            let curr_ts = person_events[i].ts_ms;
            if curr_ts - prev_ts > 1_800_000 {
                process_person_session(
                    &person_events[session_start_idx..i],
                    person_clicks,
                    &mut click_left,
                    &mut click_right,
                    &mut curr_day,
                    &mut day_sessions,
                    &mut day_events,
                    &mut day_revenue,
                    &mut day_has_purchases,
                    &mut cat_rev,
                    campaign_sink,
                    category_names,
                    person,
                    &mut user_rows,
                    &flush_day,
                );
                session_start_idx = i;
            }
            prev_ts = curr_ts;
        }
        process_person_session(
            &person_events[session_start_idx..],
            person_clicks,
            &mut click_left,
            &mut click_right,
            &mut curr_day,
            &mut day_sessions,
            &mut day_events,
            &mut day_revenue,
            &mut day_has_purchases,
            &mut cat_rev,
            campaign_sink,
            category_names,
            person,
            &mut user_rows,
            &flush_day,
        );

        if curr_day != -1 {
            flush_day(
                curr_day,
                day_sessions,
                day_events,
                day_revenue,
                day_has_purchases,
                &cat_rev,
                category_names,
                person,
                &mut user_rows,
            );
        }

        ev_start = ev_end;
    }

    user_rows
}

#[inline(always)]
fn process_person_session(
    session_events: &[LocalEvent],
    person_clicks: &[LocalClick],
    click_left: &mut usize,
    click_right: &mut usize,
    curr_day: &mut i32,
    day_sessions: &mut i64,
    day_events: &mut i64,
    day_revenue: &mut i64,
    day_has_purchases: &mut bool,
    cat_rev: &mut [i64; 256],
    campaign_sink: &mut CampaignDailySink,
    category_names: &[String],
    person: CompactId,
    user_rows: &mut Vec<UserDailyRow>,
    flush_day: &impl Fn(i32, i64, i64, i64, bool, &[i64; 256], &[String], CompactId, &mut Vec<UserDailyRow>),
) {
    let start_ms = session_events[0].ts_ms;
    let day = epoch_days(start_ms);

    if *curr_day != day {
        if *curr_day != -1 {
            flush_day(
                *curr_day,
                *day_sessions,
                *day_events,
                *day_revenue,
                *day_has_purchases,
                cat_rev,
                category_names,
                person,
                user_rows,
            );
            cat_rev.fill(0);
            *day_sessions = 0;
            *day_events = 0;
            *day_revenue = 0;
            *day_has_purchases = false;
        }
        *curr_day = day;
    }

    *day_sessions += 1;
    *day_events += session_events.len() as i64;

    let mut session_revenue = 0i64;
    let mut session_purchases = 0i64;

    for ev in session_events {
        if ev.price() != 0 {
            session_purchases += 1;
            session_revenue += i64::from(ev.price());
            cat_rev[ev.category()] += i64::from(ev.price());
            *day_has_purchases = true;
        }
    }
    *day_revenue += session_revenue;

    // Sliding window boundaries:
    // start_ms - 604_800_000 <= ts_ms <= start_ms
    let window_start = start_ms.saturating_sub(604_800_000);

    while *click_right < person_clicks.len() && person_clicks[*click_right].ts_ms <= start_ms {
        *click_right += 1;
    }
    while *click_left < *click_right && person_clicks[*click_left].ts_ms < window_start {
        *click_left += 1;
    }

    let left = *click_left;
    let right = *click_right;

    let winning_camp_idx = if left < right {
        let max_ts = person_clicks[right - 1].ts_ms;
        let mut best = &person_clicks[right - 1];
        let mut idx = right - 1;
        while idx > left && person_clicks[idx - 1].ts_ms == max_ts {
            idx -= 1;
            if person_clicks[idx].click_id.as_bytes() < best.click_id.as_bytes() {
                best = &person_clicks[idx];
            }
        }
        best.camp_idx
    } else {
        u16::MAX // organic
    };

    campaign_sink.record_credit(winning_camp_idx, day, session_purchases, session_revenue);

    if left < right {
        let count = right - left;
        if count <= 32 {
            let mut seen: [u16; 32] = [0; 32];
            let mut seen_len = 0;
            for i in left..right {
                let camp = person_clicks[i].camp_idx;
                if !seen[..seen_len].contains(&camp) {
                    seen[seen_len] = camp;
                    seen_len += 1;
                    campaign_sink.record_assist(camp, day);
                }
            }
        } else {
            let mut seen: HashSet<u16, FastBuildHasher> = HashSet::with_hasher(FastBuildHasher);
            for i in left..right {
                let camp = person_clicks[i].camp_idx;
                if seen.insert(camp) {
                    campaign_sink.record_assist(camp, day);
                }
            }
        }
    }
}

pub fn write_user_daily_shard(
    output_dir: &Path,
    shard_id: usize,
    rows: &[UserDailyRow],
    category_names: &[String],
) -> Result<()> {
    let out_dir = output_dir.join("user_daily");
    fs::create_dir_all(&out_dir)?;
    let file = File::create(out_dir.join(format!("part-{shard_id}.parquet")))?;

    let schema = user_daily_schema();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let mut user_builder = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
    let mut day_builder = Date32Builder::with_capacity(rows.len());
    let mut sess_builder = Int64Builder::with_capacity(rows.len());
    let mut ev_builder = Int64Builder::with_capacity(rows.len());
    let mut rev_builder = Int64Builder::with_capacity(rows.len());
    let mut top_cat_builder = StringBuilder::with_capacity(rows.len(), rows.len() * 16);

    for row in rows {
        user_builder.append_value(row.user_id.as_str());
        day_builder.append_value(row.day);
        sess_builder.append_value(row.sessions);
        ev_builder.append_value(row.events);
        rev_builder.append_value(row.revenue_cents);
        match row.top_category {
            Some(cat) => top_cat_builder.append_value(&category_names[cat as usize]),
            None => top_cat_builder.append_null(),
        }
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(user_builder.finish()),
            Arc::new(day_builder.finish()),
            Arc::new(sess_builder.finish()),
            Arc::new(ev_builder.finish()),
            Arc::new(rev_builder.finish()),
            Arc::new(top_cat_builder.finish()),
        ],
    )?;

    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

pub fn write_campaign_daily(output_dir: &Path, rows: Vec<CampaignDailyRow>) -> Result<()> {
    let out_dir = output_dir.join("campaign_daily");
    fs::create_dir_all(&out_dir)?;
    let file = File::create(out_dir.join("part-0.parquet"))?;

    let schema = campaign_daily_schema();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let mut camp_builder = StringBuilder::new();
    let mut chan_builder = StringBuilder::new();
    let mut day_builder = Date32Builder::new();
    let mut sess_builder = Int64Builder::new();
    let mut pur_sess_builder = Int64Builder::new();
    let mut rev_builder = Int64Builder::new();
    let mut ass_sess_builder = Int64Builder::new();

    for row in rows {
        camp_builder.append_value(&row.campaign_id);
        chan_builder.append_value(&row.channel);
        day_builder.append_value(row.day);
        sess_builder.append_value(row.sessions);
        pur_sess_builder.append_value(row.purchasing_sessions);
        rev_builder.append_value(row.revenue_cents);
        ass_sess_builder.append_value(row.assisted_sessions);
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(camp_builder.finish()),
            Arc::new(chan_builder.finish()),
            Arc::new(day_builder.finish()),
            Arc::new(sess_builder.finish()),
            Arc::new(pur_sess_builder.finish()),
            Arc::new(rev_builder.finish()),
            Arc::new(ass_sess_builder.finish()),
        ],
    )?;

    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn local_event_is_sixteen_bytes_and_preserves_validated_values() {
        assert_eq!(std::mem::size_of::<super::LocalEvent>(), 16);
        for price in [1, 250, 10_000_000] {
            for category in [0, 19, 255] {
                let input = super::RawPersonEvent {
                    person: super::CompactId::default(),
                    ts_ms: i64::MAX,
                    price_cents: price,
                    cat_idx: category,
                    is_purchase: true,
                };
                let event = super::LocalEvent::new(&input, u32::MAX);
                assert_eq!(event.price(), price as u32);
                assert_eq!(event.category(), category as usize);
                assert_eq!(event.p_idx, u32::MAX);
                assert_eq!(event.ts_ms, i64::MAX);
            }
        }
        let input = super::RawPersonEvent {
            person: super::CompactId::default(),
            ts_ms: i64::MIN,
            price_cents: 0,
            cat_idx: 255,
            is_purchase: false,
        };
        let event = super::LocalEvent::new(&input, 0);
        assert_eq!(event.price(), 0);
        assert_eq!(event.category(), 255);
        assert_eq!(event.ts_ms, i64::MIN);
    }
    #[test]
    fn word_hash_preserves_borrowed_key_lookup_at_chunk_boundaries() {
        let mut map = std::collections::HashMap::with_hasher(super::FastBuildHasher);
        let mut keys = Vec::new();
        for length in [0, 1, 7, 8, 9, 15, 16, 17, 31, 32, 100] {
            for last in [0, b'a', b'z', 255] {
                let mut key = vec![b'x'; length];
                key.push(last);
                let index = keys.len();
                map.insert(key.clone().into_boxed_slice(), index);
                keys.push(key);
            }
        }
        for (index, key) in keys.iter().enumerate() {
            assert_eq!(map.get(key.as_slice()), Some(&index));
        }
        assert_eq!(map.len(), keys.len());
    }
    #[test]
    fn date_cache_interleaving_collisions_and_invalid_dates() {
        use crate::parsing::timestamp;
        let mut cases = Vec::new();
        for year in [1969, 1970, 2000, 2026, 2028, 2100] {
            for month in [1, 2, 3, 12] {
                for day in [1, 14, 28, 29, 30, 31] {
                    cases.push(format!("{year:04}-{month:02}-{day:02}T23:59:59Z"));
                }
            }
        }
        cases.extend(["0000-01-01T00:00:00Z", "2026-02-29T00:00:00Z",
                      "2028-02-29T24:00:00Z", "2028-02-29T00:60:00Z",
                      "2028-02-29T00:00:60Z", "202x-02-28T00:00:00Z"]
                     .map(str::to_owned));
        for _ in 0..3 {
            for value in &cases {
                assert_eq!(super::parse_ts_bytes(value.as_bytes()),
                           timestamp(Some(value.as_str())), "{value}");
            }
            cases.reverse();
        }
    }
    #[test]
    fn compact_event_offsets_preserve_full_timestamps() {
        assert_eq!(std::mem::size_of::<RawEvent>(), 64);
        let start = 1_790_812_800_000;
        let mut parsed = ParsedEvents::default();
        for (ingest, ts, wide) in [
            (start - 1, Some(start + 1), false),
            (start + i64::from(i32::MAX), None, false),
            (start + i64::from(i32::MIN), Some(start), false),
            (start + i64::from(i32::MAX) + 1, Some(start), true),
            (start, Some(start + i64::from(u32::MAX) + 1), true),
            (i64::MIN, None, true),
            (start, Some(i64::MAX), true),
        ] {
            let (ingest_offset, ts_offset, flags) = parsed.encode_times(start, ingest, ts);
            let event = RawEvent {
                eid_hash: 0,
                person: CompactId::default(),
                ingest_offset,
                ts_offset,
                price_cents: 0,
                p_shard: 0,
                cat_idx: 0,
                flags,
            };
            assert_eq!(flags & WIDE_TIMES != 0, wide);
            assert_eq!(parsed.decode_times(&event, start), (ingest, ts.unwrap_or(i64::MIN)));
        }
    }
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TEST_DIR_COUNTER: AtomicUsize = AtomicUsize::new(0);

    struct TempDir(PathBuf);
    impl TempDir {
        fn new(name: &str) -> Self {
            let p = std::env::temp_dir().join(format!(
                "shopping-test-{}-{}-{}",
                name,
                std::process::id(),
                TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&p).unwrap();
            Self(p)
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn test_compact_id_long_ids_exactness() {
        let id31 = b"u123456789012345678901234567890"; // exactly 31 bytes
        let id32 = b"u1234567890123456789012345678901"; // exactly 32 bytes
        let c31 = CompactId::from_bytes(id31);
        let c32 = CompactId::from_bytes(id32);
        assert_eq!(c31.as_bytes(), id31);
        assert_eq!(c32.as_bytes(), id32);
        assert_ne!(c31, c32);
        assert!(c31 < c32);

        // 40-byte IDs differing only at the 40th byte
        let id40_a = b"uaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1";
        let id40_b = b"uaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2";
        let c40_a = CompactId::from_bytes(id40_a);
        let c40_b = CompactId::from_bytes(id40_b);
        assert_eq!(c40_a.as_bytes(), id40_a);
        assert_eq!(c40_b.as_bytes(), id40_b);
        assert_ne!(c40_a, c40_b);
        assert!(c40_a < c40_b);

        // 100-byte ID
        let id100 = vec![b'x'; 100];
        let c100 = CompactId::from_bytes(&id100);
        assert_eq!(c100.as_bytes(), &id100[..]);
    }

    #[test]
    fn test_pipeline_long_id_regression_two_40_byte_users() {
        let temp = TempDir::new("long-user");
        let data = temp.0.join("data");
        let output = temp.0.join("output");
        fs::create_dir_all(data.join("events")).unwrap();
        fs::create_dir_all(data.join("clicks")).unwrap();
        fs::create_dir_all(data.join("links")).unwrap();

        fs::write(data.join("manifest.json"), r#"{"window_start":"2027-12-19T00:00:00Z","window_end":"2027-12-20T00:00:00Z"}"#).unwrap();
        fs::write(data.join("products.csv"), "product_id,category\np1,electronics\n").unwrap();
        fs::write(data.join("campaigns.csv"), "campaign_id,channel\nc1,social\n").unwrap();

        // Two 40-byte user IDs differing only after byte 31
        let u1 = "uaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1";
        let u2 = "uaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2";
        let events_csv = format!(
            "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n\
             e1,{},2027-12-19T10:00:00Z,2027-12-19T10:05:00Z,view,p1,\n\
             e2,{},2027-12-19T11:00:00Z,2027-12-19T11:05:00Z,view,p1,\n",
            u1, u2
        );
        fs::write(data.join("events/part-0.csv"), events_csv).unwrap();

        run_pipeline(&data, &output, None, None, 2).unwrap();

        // Read user_daily parquet
        let user_daily_dir = output.join("user_daily");
        let mut user_rows = Vec::new();
        for entry in fs::read_dir(user_daily_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|ext| ext == "parquet") {
                let file = File::open(path).unwrap();
                let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
                for batch in reader {
                    let batch = batch.unwrap();
                    let users = batch.column(0).as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
                    let events = batch.column(3).as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
                    for i in 0..batch.num_rows() {
                        user_rows.push((users.value(i).to_string(), events.value(i)));
                    }
                }
            }
        }
        user_rows.sort();
        assert_eq!(user_rows.len(), 2, "Expected 2 distinct user rows for 40-byte IDs, got: {:?}", user_rows);
        assert_eq!(user_rows[0].0, u1);
        assert_eq!(user_rows[0].1, 1);
        assert_eq!(user_rows[1].0, u2);
        assert_eq!(user_rows[1].1, 1);
    }

    #[test]
    fn test_pipeline_long_click_id_tie_break_regression() {
        let temp = TempDir::new("long-click");
        let data = temp.0.join("data");
        let output = temp.0.join("output");
        fs::create_dir_all(data.join("events")).unwrap();
        fs::create_dir_all(data.join("clicks")).unwrap();
        fs::create_dir_all(data.join("links")).unwrap();

        fs::write(data.join("manifest.json"), r#"{"window_start":"2027-12-19T00:00:00Z","window_end":"2027-12-20T00:00:00Z"}"#).unwrap();
        fs::write(data.join("products.csv"), "product_id,category\np1,electronics\n").unwrap();
        fs::write(data.join("campaigns.csv"), "campaign_id,channel\nc1,social\nc2,search\n").unwrap();

        // 35-byte click IDs differing only after byte 31
        let cl1 = "click_aaaaaaaaaaaaaaaaaaaaaaaaaa_1"; // byte 34 is '1'
        let cl2 = "click_aaaaaaaaaaaaaaaaaaaaaaaaaa_2"; // byte 34 is '2'

        // Same timestamp! cl1 is lexicographically smaller than cl2, so cl1 (c1) must win!
        let clicks_csv = format!(
            "click_id,user_id,campaign_id,ts\n\
             {},u1,c2,2027-12-19T09:00:00Z\n\
             {},u1,c1,2027-12-19T09:00:00Z\n",
            cl2, cl1
        );
        fs::write(data.join("clicks/part-0.csv"), clicks_csv).unwrap();

        let events_csv = "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n\
                          e1,u1,2027-12-19T10:00:00Z,2027-12-19T10:05:00Z,purchase,p1,500\n";
        fs::write(data.join("events/part-0.csv"), events_csv).unwrap();

        run_pipeline(&data, &output, None, None, 2).unwrap();

        // Verify campaign_daily: c1 must be credited with 1 session, c2 with 0
        let c_file = File::open(output.join("campaign_daily/part-0.parquet")).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(c_file).unwrap().build().unwrap();
        let mut credited = HashMap::new();
        for batch in reader {
            let batch = batch.unwrap();
            let camps = batch.column(0).as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
            let sess = batch.column(3).as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
            for i in 0..batch.num_rows() {
                credited.insert(camps.value(i).to_string(), sess.value(i));
            }
        }
        assert_eq!(credited.get("c1"), Some(&1), "Expected c1 to win tie break with smaller click_id");
    }

    #[test]
    fn test_pipeline_long_link_id_smallest_canonical() {
        let temp = TempDir::new("long-link");
        let data = temp.0.join("data");
        let output = temp.0.join("output");
        fs::create_dir_all(data.join("events")).unwrap();
        fs::create_dir_all(data.join("clicks")).unwrap();
        fs::create_dir_all(data.join("links")).unwrap();

        fs::write(data.join("manifest.json"), r#"{"window_start":"2027-12-19T00:00:00Z","window_end":"2027-12-20T00:00:00Z"}"#).unwrap();
        fs::write(data.join("products.csv"), "product_id,category\np1,electronics\n").unwrap();
        fs::write(data.join("campaigns.csv"), "campaign_id,channel\nc1,social\n").unwrap();

        // Long ID (>31 bytes) that is byte-wise smallest in component
        let long_small = "u_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_0"; // 34 bytes
        let other = "u_zzzzzzzzzzzzzzzzzzzzzzzzzzzzzz_9"; // 34 bytes

        let links_csv = format!("{},{}\n", long_small, other);
        fs::write(data.join("links/part-0.csv"), links_csv).unwrap();

        let events_csv = format!(
            "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n\
             e1,{},2027-12-19T10:00:00Z,2027-12-19T10:05:00Z,view,p1,\n",
            other
        );
        fs::write(data.join("events/part-0.csv"), events_csv).unwrap();

        run_pipeline(&data, &output, None, None, 2).unwrap();

        // Check user_daily: user_id must be the full 34-byte long_small!
        let user_daily_dir = output.join("user_daily");
        let mut user_names = Vec::new();
        for entry in fs::read_dir(user_daily_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|ext| ext == "parquet") {
                let file = File::open(path).unwrap();
                let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
                for batch in reader {
                    let batch = batch.unwrap();
                    let users = batch.column(0).as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
                    for i in 0..batch.num_rows() {
                        user_names.push(users.value(i).to_string());
                    }
                }
            }
        }
        assert_eq!(user_names, vec![long_small.to_string()]);
    }
}
