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

pub struct UserDailyRow {
    pub user_id: String,
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue_cents: i64,
    pub top_category: Option<String>,
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


#[derive(Default)]
struct CampaignDailyAccumulator {
    sessions: i64,
    purchasing_sessions: i64,
    revenue_cents: i64,
    assisted_sessions: i64,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct CompactId {
    pub len: u8,
    pub bytes: [u8; 31],
}

impl CompactId {
    #[inline(always)]
    pub fn from_bytes(b: &[u8]) -> Self {
        let len = b.len().min(31);
        let mut bytes = [0u8; 31];
        bytes[..len].copy_from_slice(&b[..len]);
        CompactId {
            len: len as u8,
            bytes,
        }
    }
    #[inline(always)]
    pub fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.bytes[..self.len as usize]) }
    }
    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }
}

// 72-byte compact RawEvent with 128-bit event ID hash and sentinel fields
struct RawEvent {
    eid_hash: u128,
    user_id: CompactId, // len == 0 if None
    ingest_ms: i64,
    ts_ms: i64, // i64::MIN if None
    price_cents: i32,
    cat_idx: u8,
    is_purchase: bool,
    is_valid: bool,
}

struct RawClick {
    click_id: CompactId,
    user_id: CompactId,
    camp_idx: u16,
    ts_ms: i64,
}

struct EventDedupEntry {
    ingest_ms: i64,
    user_id: CompactId,
    ts_ms: i64,
    price_cents: i32,
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

// Ultra-compact 16-byte event for instant integer-keyed sorting
#[derive(Clone, Copy)]
struct LocalEvent {
    ts_ms: i64,
    price_cents: i32,
    p_idx: u16,
    cat_idx: u8,
    is_purchase: bool,
}

#[derive(Clone, Copy)]
struct LocalClick {
    ts_ms: i64,
    click_id: CompactId,
    p_idx: u16,
    camp_idx: u16,
}

struct FastHasher(u64);
impl Hasher for FastHasher {
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline(always)]
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0.wrapping_mul(1099511628211)) ^ u64::from(b);
        }
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
    let mut h1 = 14695981039346656037u64;
    let mut h2 = 1099511628211u64;
    for &b in bytes {
        h1 = (h1.wrapping_mul(1099511628211)) ^ u64::from(b);
        h2 = (h2.wrapping_mul(14695981039346656037)) ^ u64::from(b);
    }
    let shard = (h1 as usize) % NUM_SHARDS;
    (((h1 as u128) << 64) | (h2 as u128), shard)
}

pub struct FrozenIdentity {
    shards: Vec<HashMap<Box<[u8]>, Box<[u8]>, FastBuildHasher>>,
    unique_count: usize,
}

impl FrozenIdentity {
    pub fn empty() -> Self {
        Self {
            shards: (0..NUM_SHARDS).map(|_| HashMap::with_hasher(FastBuildHasher)).collect(),
            unique_count: 0,
        }
    }

    #[inline(always)]
    pub fn get<'a>(&'a self, user: &'a [u8]) -> &'a [u8] {
        if self.shards.is_empty() {
            return user;
        }
        let shard = get_shard_bytes(user);
        match self.shards[shard].get(user) {
            Some(canon) => canon.as_ref(),
            None => user,
        }
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

    let mut shards: Vec<HashMap<Box<[u8]>, Box<[u8]>, FastBuildHasher>> =
        (0..NUM_SHARDS).map(|_| HashMap::with_hasher(FastBuildHasher)).collect();

    let unique_count = names.len();
    for i in 0..names.len() {
        let root = find(i as u32, &mut parents);
        let best_name = names[minima[root as usize] as usize];
        let orig_name = names[i];
        if orig_name != best_name {
            let shard = get_shard_bytes(orig_name);
            shards[shard].insert(orig_name.to_vec().into_boxed_slice(), best_name.to_vec().into_boxed_slice());
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
    let y = d(0)? * 1000 + d(1)? * 100 + d(2)? * 10 + d(3)?;
    let m = d(5)? * 10 + d(6)?;
    let day = d(8)? * 10 + d(9)?;
    let h = d(11)? * 10 + d(12)?;
    let mi = d(14)? * 10 + d(15)?;
    let s = d(17)? * 10 + d(18)?;

    if h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let days = civil_to_days(y, m, day)?;
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
fn parse_window_ts_bytes(b: Option<&[u8]>, start: i128, end: i128) -> Option<i64> {
    let ts = parse_ts_bytes(b?)?;
    let ts_i128 = i128::from(ts);
    if ts_i128 >= start && ts_i128 < end {
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

    if !line.contains(&b'"') {
        let mut count = 0;
        let mut start = 0;
        let len = line.len();
        while start <= len {
            if count >= N {
                return false;
            }
            let mut end = start;
            while end < len && line[end] != b',' {
                end += 1;
            }
            let raw = &line[start..end];
            fields[count] = if raw.is_empty() { None } else { Some(raw) };
            count += 1;
            if end == len {
                break;
            }
            start = end + 1;
        }
        return count == N;
    }

    let mut count = 0;
    let mut start = 0;
    let len = line.len();

    while start <= len {
        if count >= N {
            return false;
        }
        let mut end = start;
        while end < len && line[end] != b',' {
            end += 1;
        }
        let raw = &line[start..end];
        let val = if raw.is_empty() {
            None
        } else if raw[0] == b'"' {
            if raw.len() >= 2 && raw[raw.len() - 1] == b'"' {
                let inner = &raw[1..raw.len() - 1];
                if inner.contains(&b'"') {
                    return false;
                }
                if inner.is_empty() { None } else { Some(inner) }
            } else {
                return false;
            }
        } else {
            if raw.contains(&b'"') {
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
            if let Some(pos) = buf[start..available].iter().position(|&b| b == b'\n') {
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
    let ids = load_parallel_identity(&data_dir.join("links"), workers_count)?;
    eprintln!("[identity] elapsed: {:.3}ms, unique identities: {}", t_identity_start.elapsed().as_secs_f64() * 1000.0, ids.len());

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
    let (worker_person_events, worker_person_clicks) = std::thread::scope(|scope| -> Result<(Vec<Vec<Vec<RawPersonEvent>>>, Vec<Vec<Vec<RawPersonClick>>>)> {
        // Spawn clicks pipeline concurrently
        let click_handle = scope.spawn(|| -> Result<Vec<Vec<Vec<RawPersonClick>>>> {
            let t_clicks_start = std::time::Instant::now();
            let click_files = list_csv_files(&data_dir.join("clicks"))?;
            let click_chunks = Arc::new(plan_chunks(&click_files, 16 * 1024 * 1024)?);
            let next_chunk = Arc::new(AtomicUsize::new(0));
            let cancelled = Arc::new(AtomicBool::new(false));
            let click_workers_count = (workers_count / 2).clamp(1, 4);

            let worker_click_shards: Vec<Vec<Vec<RawClick>>> = std::thread::scope(|c_scope| {
                let mut handles = Vec::new();
                for _ in 0..click_workers_count {
                    let click_chunks = Arc::clone(&click_chunks);
                    let next_chunk = Arc::clone(&next_chunk);
                    let cancelled = Arc::clone(&cancelled);
                    let campaign_to_idx = &campaign_to_idx;

                    handles.push(c_scope.spawn(move || -> Result<Vec<Vec<RawClick>>> {
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
                                    i128::from(window_start) - 604_800_000,
                                    i128::from(window_end),
                                );
                                let camp_idx_opt = fields[2].and_then(|c| campaign_to_idx.get(&CompactId::from_bytes(c)).copied());
                                let is_valid = fields[1].is_some() && ts_ms.is_some() && camp_idx_opt.is_some();

                                if is_valid {
                                    let click_id = CompactId::from_bytes(click_id_bytes);
                                    let shard_idx = get_shard_bytes(click_id.as_bytes());
                                    local_shards[shard_idx].push(RawClick {
                                        click_id,
                                        user_id: CompactId::from_bytes(fields[1].unwrap()),
                                        camp_idx: camp_idx_opt.unwrap(),
                                        ts_ms: ts_ms.unwrap(),
                                    });
                                }
                                Ok(())
                            })?;
                        }
                        Ok(local_shards)
                    }));
                }
                let mut results = Vec::new();
                for h in handles {
                    results.push(h.join().unwrap()?);
                }
                Ok::<Vec<Vec<Vec<RawClick>>>, Box<dyn std::error::Error + Send + Sync>>(results)
            })?;

            // Deduplication of clicks
            let worker_person_clicks: Vec<Vec<Vec<RawPersonClick>>> = std::thread::scope(|c_scope| {
                let shards_per_worker = (NUM_SHARDS + click_workers_count - 1) / click_workers_count;
                let mut handles = Vec::new();
                let worker_click_shards = &worker_click_shards;
                let ids = &ids;

                for worker_idx in 0..click_workers_count {
                    let start_shard = worker_idx * shards_per_worker;
                    let end_shard = (start_shard + shards_per_worker).min(NUM_SHARDS);

                    handles.push(c_scope.spawn(move || -> Vec<Vec<RawPersonClick>> {
                        let mut local_person_shards: Vec<Vec<RawPersonClick>> =
                            (0..NUM_SHARDS).map(|_| Vec::with_capacity(256)).collect();

                        let mut seen_clicks: HashSet<CompactId, FastBuildHasher> =
                            HashSet::with_capacity_and_hasher(16384, FastBuildHasher);

                        for shard_idx in start_shard..end_shard {
                            seen_clicks.clear();

                            for worker_shards in worker_click_shards {
                                for cl in &worker_shards[shard_idx] {
                                    if seen_clicks.insert(cl.click_id) {
                                        let person_bytes = ids.get(cl.user_id.as_bytes());
                                        let person_compact = CompactId::from_bytes(person_bytes);
                                        let p_shard = get_shard_bytes(person_compact.as_bytes());
                                        local_person_shards[p_shard].push(RawPersonClick {
                                            person: person_compact,
                                            ts_ms: cl.ts_ms,
                                            camp_idx: cl.camp_idx,
                                            click_id: cl.click_id,
                                        });
                                    }
                                }
                            }
                        }
                        local_person_shards
                    }));
                }
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            });
            eprintln!("[clicks_total] elapsed: {:.3}ms", t_clicks_start.elapsed().as_secs_f64() * 1000.0);
            Ok(worker_person_clicks)
        });

        // Event Parsing
        let t_events_parse_start = std::time::Instant::now();
        let event_files = list_csv_files(&data_dir.join("events"))?;
        let event_chunks = Arc::new(plan_chunks(&event_files, 16 * 1024 * 1024)?);
        let next_chunk = Arc::new(AtomicUsize::new(0));
        let cancelled = Arc::new(AtomicBool::new(false));

        let worker_event_shards: Vec<Vec<Vec<RawEvent>>> = std::thread::scope(|e_scope| {
            let mut handles = Vec::new();
            for _ in 0..workers_count {
                let event_chunks = Arc::clone(&event_chunks);
                let next_chunk = Arc::clone(&next_chunk);
                let cancelled = Arc::clone(&cancelled);
                let products_map = &products_map;
                let direct_products = &direct_products;

                handles.push(e_scope.spawn(move || -> Result<Vec<Vec<RawEvent>>> {
                    let mut local_shards: Vec<Vec<RawEvent>> = (0..NUM_SHARDS).map(|_| Vec::with_capacity(4096)).collect();

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
                            let ts_ms_opt = parse_window_ts_bytes(fields[2], i128::from(window_start), i128::from(window_end));
                            let is_purchase = fields[4] == Some(b"purchase");
                            let price_cents = if is_purchase {
                                parse_unsigned_bytes(fields[6], 10000000).filter(|v| *v > 0)
                            } else {
                                None
                            };
                            let is_valid = fields[1].is_some()
                                && ts_ms_opt.is_some()
                                && fields[5].is_some()
                                && matches!(fields[4], Some(b"view" | b"cart" | b"purchase"))
                                && (!is_purchase || price_cents.is_some());

                            let category_idx = if let Some(pid) = fields[5] {
                                if pid.len() > 1 && pid[0] == b'p' {
                                    let mut num: usize = 0;
                                    let mut valid = true;
                                    for &c in &pid[1..] {
                                        if c.is_ascii_digit() {
                                            num = num * 10 + (c - b'0') as usize;
                                        } else {
                                            valid = false;
                                            break;
                                        }
                                    }
                                    if valid && num < direct_products.len() {
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
                            } else {
                                unknown_idx
                            };

                            let (eid_hash, shard_idx) = hash_event_id_128(event_id_bytes);
                            let user_id = fields[1].map(CompactId::from_bytes).unwrap_or_default();

                            local_shards[shard_idx].push(RawEvent {
                                eid_hash,
                                user_id,
                                ingest_ms,
                                ts_ms: ts_ms_opt.unwrap_or(i64::MIN),
                                price_cents: price_cents.unwrap_or(0) as i32,
                                cat_idx: category_idx,
                                is_purchase,
                                is_valid,
                            });
                            Ok(())
                        })?;
                    }
                    Ok(local_shards)
                }));
            }
            let mut results = Vec::new();
            for h in handles {
                results.push(h.join().unwrap()?);
            }
            Ok::<Vec<Vec<Vec<RawEvent>>>, Box<dyn std::error::Error + Send + Sync>>(results)
        })?;
        let t_events_parse_elapsed = t_events_parse_start.elapsed().as_secs_f64() * 1000.0;
        eprintln!("[events_parse] elapsed: {:.3}ms", t_events_parse_elapsed);

        // 6. Lock-Free Parallel Deduplication of Events and Routing to Person Shards
        let t_events_dedup_start = std::time::Instant::now();
        let worker_person_events: Vec<Vec<Vec<RawPersonEvent>>> = std::thread::scope(|e_scope| {
            let shards_per_worker = (NUM_SHARDS + workers_count - 1) / workers_count;
            let mut handles = Vec::new();
            let worker_event_shards = &worker_event_shards;
            let ids = &ids;

            for worker_idx in 0..workers_count {
                let start_shard = worker_idx * shards_per_worker;
                let end_shard = (start_shard + shards_per_worker).min(NUM_SHARDS);

                handles.push(e_scope.spawn(move || -> Vec<Vec<RawPersonEvent>> {
                    let mut local_person_shards: Vec<Vec<RawPersonEvent>> =
                        (0..NUM_SHARDS).map(|_| Vec::with_capacity(1024)).collect();

                    let mut dedup_map: HashMap<u128, EventDedupEntry, FastBuildHasher> =
                        HashMap::with_capacity_and_hasher(65536, FastBuildHasher);

                    for shard_idx in start_shard..end_shard {
                        dedup_map.clear();

                        for worker_shards in worker_event_shards {
                            for ev in &worker_shards[shard_idx] {
                                match dedup_map.get_mut(&ev.eid_hash) {
                                    Some(existing) => {
                                        if ev.ingest_ms < existing.ingest_ms {
                                            *existing = EventDedupEntry {
                                                ingest_ms: ev.ingest_ms,
                                                user_id: ev.user_id,
                                                ts_ms: ev.ts_ms,
                                                price_cents: ev.price_cents,
                                                cat_idx: ev.cat_idx,
                                                is_purchase: ev.is_purchase,
                                                is_valid: ev.is_valid,
                                            };
                                        }
                                    }
                                    None => {
                                        dedup_map.insert(
                                            ev.eid_hash,
                                            EventDedupEntry {
                                                ingest_ms: ev.ingest_ms,
                                                user_id: ev.user_id,
                                                ts_ms: ev.ts_ms,
                                                price_cents: ev.price_cents,
                                                cat_idx: ev.cat_idx,
                                                is_purchase: ev.is_purchase,
                                                is_valid: ev.is_valid,
                                            },
                                        );
                                    }
                                }
                            }
                        }

                        for entry in dedup_map.values() {
                            if entry.is_valid && entry.ts_ms != i64::MIN {
                                let ts = entry.ts_ms;
                                if entry.ingest_ms - ts <= 86_400_000 {
                                    let user = entry.user_id;
                                    let person_bytes = ids.get(user.as_bytes());
                                    let person_compact = CompactId::from_bytes(person_bytes);
                                    let p_shard = get_shard_bytes(person_compact.as_bytes());
                                    local_person_shards[p_shard].push(RawPersonEvent {
                                        person: person_compact,
                                        ts_ms: ts,
                                        price_cents: entry.price_cents,
                                        cat_idx: entry.cat_idx,
                                        is_purchase: entry.is_purchase,
                                    });
                                }
                            }
                        }
                    }
                    local_person_shards
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        eprintln!("[events_dedup] elapsed: {:.3}ms", t_events_dedup_start.elapsed().as_secs_f64() * 1000.0);

        let worker_person_clicks = click_handle.join().unwrap()?;
        Ok((worker_person_events, worker_person_clicks))
    })?;

    // 9. Parallel Execution of Analytics across Person Shards using 16-byte integer-keyed local events
    let t_analytics_start = std::time::Instant::now();
    let out_user_dir = output_dir.join("user_daily");
    fs::create_dir_all(&out_user_dir)?;

    let partial_results: Vec<Result<HashMap<(u16, i32), CampaignDailyAccumulator>>> =
        std::thread::scope(|scope| {
            let shards_per_worker = (NUM_SHARDS + workers_count - 1) / workers_count;
            let mut handles = Vec::new();
            let worker_person_events = &worker_person_events;
            let worker_person_clicks = &worker_person_clicks;
            let category_names = &category_names;

            for worker_idx in 0..workers_count {
                let start_shard = worker_idx * shards_per_worker;
                let end_shard = (start_shard + shards_per_worker).min(NUM_SHARDS);

                handles.push(scope.spawn(move || -> Result<HashMap<(u16, i32), CampaignDailyAccumulator>> {
                    let mut user_rows = Vec::new();
                    let mut campaign_daily_map: HashMap<(u16, i32), CampaignDailyAccumulator> = HashMap::new();

                    let mut shard_person_map: HashMap<CompactId, u16, FastBuildHasher> =
                        HashMap::with_capacity_and_hasher(1024, FastBuildHasher);
                    let mut shard_person_names: Vec<CompactId> = Vec::with_capacity(1024);
                    let mut local_events: Vec<LocalEvent> = Vec::with_capacity(65536);
                    let mut local_clicks: Vec<LocalClick> = Vec::with_capacity(16384);

                    for shard_idx in start_shard..end_shard {
                        shard_person_map.clear();
                        shard_person_names.clear();
                        local_events.clear();
                        local_clicks.clear();

                        for w_events in worker_person_events {
                            for ev in &w_events[shard_idx] {
                                let p_idx = match shard_person_map.get(&ev.person) {
                                    Some(&idx) => idx,
                                    None => {
                                        let idx = shard_person_names.len() as u16;
                                        shard_person_names.push(ev.person);
                                        shard_person_map.insert(ev.person, idx);
                                        idx
                                    }
                                };
                                local_events.push(LocalEvent {
                                    ts_ms: ev.ts_ms,
                                    price_cents: ev.price_cents,
                                    p_idx,
                                    cat_idx: ev.cat_idx,
                                    is_purchase: ev.is_purchase,
                                });
                            }
                        }

                        for w_clicks in worker_person_clicks {
                            for cl in &w_clicks[shard_idx] {
                                if let Some(&p_idx) = shard_person_map.get(&cl.person) {
                                    local_clicks.push(LocalClick {
                                        ts_ms: cl.ts_ms,
                                        click_id: cl.click_id,
                                        p_idx,
                                        camp_idx: cl.camp_idx,
                                    });
                                }
                            }
                        }

                        // Ultra-fast packed 64-bit integer key sort: (p_idx << 32 | ts_offset) compiles to single cmp instruction
                        local_events.sort_unstable_by_key(|e| ((e.p_idx as u64) << 32) | ((e.ts_ms - window_start) as u32 as u64));
                        local_clicks.sort_unstable_by(|a, b| {
                            (a.p_idx, a.ts_ms, a.click_id.as_bytes()).cmp(&(b.p_idx, b.ts_ms, b.click_id.as_bytes()))
                        });

                        let (u_part, c_part) = execute_shard_local(
                            &local_events,
                            &local_clicks,
                            &shard_person_names,
                            category_names,
                        );
                        user_rows.extend(u_part);
                        for (k, acc) in c_part {
                            let entry = campaign_daily_map.entry(k).or_default();
                            entry.sessions += acc.sessions;
                            entry.purchasing_sessions += acc.purchasing_sessions;
                            entry.revenue_cents += acc.revenue_cents;
                            entry.assisted_sessions += acc.assisted_sessions;
                        }
                    }

                    if worker_idx == 0 || !user_rows.is_empty() {
                        write_user_daily_shard(output_dir, worker_idx, user_rows)?;
                    }

                    Ok(campaign_daily_map)
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

    let mut merged_campaign_map: HashMap<(u16, i32), CampaignDailyAccumulator> = HashMap::new();

    for c_map_res in partial_results {
        let c_map = c_map_res?;
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
    eprintln!("[analytics_sessions_sort_parquet] elapsed: {:.3}ms", t_analytics_start.elapsed().as_secs_f64() * 1000.0);

    // 10. Write CampaignDaily Parquet File
    let t_parquet_start = std::time::Instant::now();
    write_campaign_daily(output_dir, campaign_rows)?;
    eprintln!("[campaign_parquet_write] elapsed: {:.3}ms", t_parquet_start.elapsed().as_secs_f64() * 1000.0);
    eprintln!("[pipeline_total] elapsed: {:.3}ms", t_pipeline_start.elapsed().as_secs_f64() * 1000.0);

    Ok(())
}

fn execute_shard_local(
    events: &[LocalEvent],
    clicks: &[LocalClick],
    person_names: &[CompactId],
    category_names: &[String],
) -> (Vec<UserDailyRow>, HashMap<(u16, i32), CampaignDailyAccumulator>) {
    let mut user_rows = Vec::new();
    let mut campaign_daily_map: HashMap<(u16, i32), CampaignDailyAccumulator> = HashMap::new();

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
        let person_name = person_names[p_idx as usize].as_str();
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
                         person_name: &str,
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
                best_cat.map(|c| category_names[c as usize].clone())
            };
            user_rows.push(UserDailyRow {
                user_id: person_name.to_string(),
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
                    &mut campaign_daily_map,
                    category_names,
                    person_name,
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
            &mut campaign_daily_map,
            category_names,
            person_name,
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
                person_name,
                &mut user_rows,
            );
        }

        ev_start = ev_end;
    }

    (user_rows, campaign_daily_map)
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
    campaign_daily_map: &mut HashMap<(u16, i32), CampaignDailyAccumulator>,
    category_names: &[String],
    person_name: &str,
    user_rows: &mut Vec<UserDailyRow>,
    flush_day: &impl Fn(i32, i64, i64, i64, bool, &[i64; 256], &[String], &str, &mut Vec<UserDailyRow>),
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
                person_name,
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
        if ev.is_purchase {
            session_purchases += 1;
            session_revenue += i64::from(ev.price_cents);
            cat_rev[ev.cat_idx as usize] += i64::from(ev.price_cents);
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

    let credited_entry = campaign_daily_map.entry((winning_camp_idx, day)).or_default();
    credited_entry.sessions += 1;
    if session_purchases > 0 {
        credited_entry.purchasing_sessions += 1;
    }
    credited_entry.revenue_cents += session_revenue;

    if left < right {
        let count = right - left;
        if count <= 8 {
            let mut seen: [u16; 8] = [0; 8];
            let mut seen_len = 0;
            for i in left..right {
                let camp = person_clicks[i].camp_idx;
                if !seen[..seen_len].contains(&camp) {
                    seen[seen_len] = camp;
                    seen_len += 1;
                    let assist_entry = campaign_daily_map.entry((camp, day)).or_default();
                    assist_entry.assisted_sessions += 1;
                }
            }
        } else {
            let mut seen: HashSet<u16, FastBuildHasher> = HashSet::with_hasher(FastBuildHasher);
            for i in left..right {
                let camp = person_clicks[i].camp_idx;
                if seen.insert(camp) {
                    let assist_entry = campaign_daily_map.entry((camp, day)).or_default();
                    assist_entry.assisted_sessions += 1;
                }
            }
        }
    }
}

pub fn write_user_daily_shard(output_dir: &Path, shard_id: usize, rows: Vec<UserDailyRow>) -> Result<()> {
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
        user_builder.append_value(&row.user_id);
        day_builder.append_value(row.day);
        sess_builder.append_value(row.sessions);
        ev_builder.append_value(row.events);
        rev_builder.append_value(row.revenue_cents);
        match row.top_category {
            Some(ref cat) => top_cat_builder.append_value(cat),
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
