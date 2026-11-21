mod simple_identity;
mod fast_records;
mod click_index;
use simple_identity::SimpleUsers;
use fast_records::{FastEvent, EXTRA_PERSON};

use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::hash::{BuildHasher, Hasher};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
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

// Keep common IDs inline while carrying an exact pointer/length for longer IDs.
// The final byte is the inline length, or has its high bit set for an external
// allocation. Rust allocations cannot exceed isize::MAX, leaving that bit free.
#[derive(Clone, Copy)]
pub struct CompactId {
    bytes: [u8; 16],
}

impl CompactId {
    #[inline(always)]
    pub fn from_bytes(b: &[u8]) -> Self {
        let mut bytes = [0u8; 16];
        if b.len() <= 15 {
            bytes[..b.len()].copy_from_slice(b);
            bytes[15] = b.len() as u8;
        } else {
            // Preserve the existing process-lifetime ownership: IDs are copied
            // between worker buffers and remain valid until the executable exits.
            let leaked: &'static [u8] = Box::leak(b.to_vec().into_boxed_slice());
            bytes[..8].copy_from_slice(&(leaked.as_ptr() as u64).to_le_bytes());
            bytes[8..].copy_from_slice(&((leaked.len() as u64) | (1 << 63)).to_le_bytes());
        }
        Self { bytes }
    }

    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        if self.bytes[15] & 0x80 == 0 {
            &self.bytes[..self.bytes[15] as usize]
        } else {
            let ptr = u64::from_le_bytes(self.bytes[..8].try_into().unwrap()) as *const u8;
            let tagged_len = u64::from_le_bytes(self.bytes[8..].try_into().unwrap());
            let len = (tagged_len & !(1 << 63)) as usize;
            // The allocation is immutable and leaked by from_bytes. Copying an
            // ID preserves its pointer, and returned borrows cannot outlive self.
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

// Inline IDs are zero-padded with their length in the final byte, so equal
// representations mean equal IDs. External IDs (16+ bytes) never equal inline
// ones, but two external copies may live in different allocations.
impl PartialEq for CompactId {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        if self.bytes == other.bytes {
            return true;
        }
        if (self.bytes[15] | other.bytes[15]) & 0x80 == 0 {
            return false;
        }
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
        if self.bytes[15] & 0x80 == 0 {
            state.write_u128(u128::from_le_bytes(self.bytes));
        } else {
            state.write(self.as_bytes());
        }
    }
}

impl Default for CompactId {
    #[inline(always)]
    fn default() -> Self {
        CompactId { bytes: [0u8; 16] }
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
    #[inline(always)]
    fn encode_times(&mut self, start: i64, ingest: i64, ts: Option<i64>) -> (u32, i32, u8) {
        let d_ingest = ingest.wrapping_sub(start);
        let (d_ts, missing) = match ts {
            Some(t) => (t.wrapping_sub(start), 0),
            None => (0, NO_EVENT_TS),
        };
        if (d_ingest as i32 as i64 == d_ingest) & (d_ts as i32 as i64 == d_ts) {
            ((d_ingest as i32) as u32, d_ts as i32, missing)
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

    #[inline(always)]
    fn decode_ingest(&self, event: &RawEvent, start: i64) -> i64 {
        if event.flags & WIDE_TIMES != 0 {
            self.wide_times[event.ingest_offset as usize].0
        } else {
            start + i64::from(event.ingest_offset as i32)
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

#[derive(Clone, Copy)]
struct DedupWinner {
    ingest_ms: i64,
    worker_idx: u16,
    event_idx: u32,
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
        // Fast path for 8-16 byte slices (typical ID length) avoids chunks_exact overhead.
        let len = bytes.len();
        let ptr = bytes.as_ptr();
        if len >= 8 && len <= 16 {
            let word = unsafe { (ptr as *const u64).read_unaligned().to_le() };
            self.0 = (self.0 ^ word).rotate_left(27).wrapping_mul(0x3c79ac492ba7b653);
            let rem_len = len - 8;
            if rem_len > 0 {
                let mut tail = [0u8; 8];
                unsafe {
                    std::ptr::copy_nonoverlapping(ptr.add(8), tail.as_mut_ptr(), rem_len);
                }
                let word = u64::from_le_bytes(tail);
                self.0 = (self.0 ^ word).rotate_left(27).wrapping_mul(0x3c79ac492ba7b653);
            }
        } else {
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
        }
        self.0 ^= (len as u64).wrapping_mul(0x1c69b3f74ac4ae35);
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


const NUM_SHARDS: usize = 64;
// Event IDs and people have independent partitioning requirements. Smaller
// event partitions keep each deduplication table bounded without multiplying
// person buffers or fragmenting a person's analytics work.
const NUM_EVENT_SHARDS: usize = 512;

struct ShardJobs {
    order: Vec<usize>,
    next: AtomicUsize,
}

impl ShardJobs {
    fn new(weights: Vec<usize>) -> Self {
        let mut order: Vec<usize> = (0..weights.len()).collect();
        order.sort_unstable_by_key(|&index| (std::cmp::Reverse(weights[index]), index));
        Self { order, next: AtomicUsize::new(0) }
    }

    fn next(&self) -> Option<usize> {
        self.order.get(self.next.fetch_add(1, Ordering::Relaxed)).copied()
    }
}

#[inline(always)]
fn get_shard_bytes(key: &[u8]) -> usize {
    (identity_hash(key) as usize) & (NUM_SHARDS - 1)
}

#[inline(always)]
fn identity_hash(key: &[u8]) -> u64 {
    let mut hasher = FastBuildHasher.build_hasher();
    hasher.write(key);
    hasher.finish()
}

#[inline(always)]
fn hash_event_id_128(bytes: &[u8]) -> (u128, usize) {
    // Word-at-a-time double-FNV: same multipliers as before but process 8 bytes
    // per iteration to cut loop overhead ~8x. Event IDs in benchmark datasets
    // are typically 8-16 bytes (e.g. 9-11 bytes); an unrolled fast path avoids
    // chunks_exact iterator and array allocation for these common lengths.
    const M1: u64 = 1099511628211;
    const M2: u64 = 14695981039346656037;
    let mut h1 = M2;
    let mut h2 = M1;
    let len = bytes.len();
    let ptr = bytes.as_ptr();

    if len >= 8 && len <= 16 {
        let w0 = unsafe { (ptr as *const u64).read_unaligned().to_le() };
        h1 = h1.wrapping_mul(M1) ^ w0;
        h2 = h2.wrapping_mul(M2) ^ w0;

        let rem_len = len - 8;
        if rem_len > 0 {
            let mut tail = [0u8; 8];
            unsafe {
                std::ptr::copy_nonoverlapping(ptr.add(8), tail.as_mut_ptr(), rem_len);
            }
            let w1 = u64::from_le_bytes(tail);
            h1 = h1.wrapping_mul(M1) ^ w1;
            h2 = h2.wrapping_mul(M2) ^ w1;
        }
    } else {
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
    }
    // Length mixing prevents collisions between differently-padded inputs.
    h1 ^= (len as u64).wrapping_mul(0x1c69b3f74ac4ae35);
    h2 ^= (len as u64).wrapping_mul(0x517cc1b727220a95);
    // Fold and avalanche every ID byte before choosing a partition. Low
    // FNV bits alone depend heavily on the first/last word's low bytes.
    let partition_hash = FastHasher(h1 ^ h2.rotate_left(17)).finish();
    let shard = (partition_hash as usize) & (NUM_EVENT_SHARDS - 1);
    (((h1 as u128) << 64) | (h2 as u128), shard)
}

struct ResolvedPerson {
    // Frozen once per linked ID, then copied directly into parsed rows.
    id: CompactId,
    shard: u8,
}

// Common short aliases and canonical IDs fit in one contiguous 33-byte cell.
// Zero key length marks an empty cell; valid link endpoints are nonempty.
#[derive(Clone, Copy, Default)]
struct ShortIdentitySlot {
    key_len: u8,
    key: [u8; 15],
    canonical_len: u8,
    canonical: [u8; 15],
    person_shard: u8,
}

struct ShortIdentityTable {
    slots: Vec<ShortIdentitySlot>,
}

impl ShortIdentityTable {
    fn with_entries(entries: usize) -> Self {
        let capacity = if entries == 0 { 0 } else {
            // At most 70% occupied, with an empty cell terminating every miss.
            (entries + entries.div_ceil(2)).next_power_of_two()
        };
        let slots = vec![ShortIdentitySlot::default(); capacity];
        // Every parsed row probes a random slot; huge pages avoid a TLB miss
        // on top of the cache miss. Advice is applied before any slot is written.
        advise_huge_pages(slots.as_ptr().cast(), capacity * std::mem::size_of::<ShortIdentitySlot>());
        Self { slots }
    }

    #[inline(always)]
    fn prefetch(&self, hash: u64) {
        #[cfg(target_arch = "x86_64")]
        if !self.slots.is_empty() {
            let index = hash as usize & (self.slots.len() - 1);
            // Slots are 33 bytes and unaligned, so a slot can span two lines.
            unsafe {
                let slot = self.slots.as_ptr().add(index).cast::<i8>();
                std::arch::x86_64::_mm_prefetch(slot, std::arch::x86_64::_MM_HINT_T0);
                std::arch::x86_64::_mm_prefetch(slot.add(32), std::arch::x86_64::_MM_HINT_T0);
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        let _ = hash;
    }

    fn insert(&mut self, key: &[u8], canonical: &[u8], hash: u64) {
        debug_assert!(!key.is_empty() && key.len() <= 15 && canonical.len() <= 15);
        let mask = self.slots.len() - 1;
        let mut index = hash as usize & mask;
        while self.slots[index].key_len != 0 { index = (index + 1) & mask; }
        let slot = &mut self.slots[index];
        slot.key_len = key.len() as u8;
        slot.key[..key.len()].copy_from_slice(key);
        slot.canonical_len = canonical.len() as u8;
        slot.canonical[..canonical.len()].copy_from_slice(canonical);
        slot.person_shard = get_shard_bytes(canonical) as u8;
    }

    #[inline(always)]
    fn get(&self, key: &[u8], hash: u64) -> Option<&ShortIdentitySlot> {
        if self.slots.is_empty() || key.is_empty() || key.len() > 15 { return None; }
        let mask = self.slots.len() - 1;
        let mut index = hash as usize & mask;
        loop {
            let slot = &self.slots[index];
            if slot.key_len == 0 { return None; }
            if slot.key_len as usize == key.len() && &slot.key[..key.len()] == key {
                return Some(slot);
            }
            index = (index + 1) & mask;
        }
    }
}

pub struct FrozenIdentity {
    short: ShortIdentityTable,
    shards: Vec<HashMap<Box<[u8]>, ResolvedPerson, FastBuildHasher>>,
    fallback_count: usize,
    unique_count: usize,
}

impl FrozenIdentity {
    pub fn empty() -> Self {
        Self {
            short: ShortIdentityTable::with_entries(0),
            shards: (0..NUM_SHARDS).map(|_| HashMap::with_hasher(FastBuildHasher)).collect(),
            fallback_count: 0,
            unique_count: 0,
        }
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn get<'a>(&'a self, user: &'a [u8]) -> &'a [u8] {
        if self.unique_count == 0 {
            return user;
        }
        let hash = identity_hash(user);
        if let Some(slot) = self.short.get(user, hash) {
            return &slot.canonical[..slot.canonical_len as usize];
        }
        let shard = hash as usize & (NUM_SHARDS - 1);
        match self.shards[shard].get(user) {
            Some(canon) => canon.id.as_bytes(),
            None => user,
        }
    }

    #[inline(always)]
    fn prefetch(&self, hash: u64) {
        self.short.prefetch(hash);
    }

    #[inline(always)]
    fn get_compact_with_shard(&self, user: &[u8]) -> (CompactId, u8) {
        self.resolve_hashed(user, identity_hash(user))
    }

    #[inline(always)]
    fn resolve_hashed(&self, user: &[u8], hash: u64) -> (CompactId, u8) {
        let shard = hash as usize & (NUM_SHARDS - 1);
        if let Some(slot) = self.short.get(user, hash) {
            return (CompactId::from_bytes(&slot.canonical[..slot.canonical_len as usize]), slot.person_shard);
        }
        if self.fallback_count != 0 {
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

#[cfg(target_os = "linux")]
fn advise_huge_pages(ptr: *const u8, len: usize) {
    unsafe extern "C" {
        fn madvise(addr: *mut std::ffi::c_void, len: usize, advice: i32) -> i32;
    }
    const MADV_HUGEPAGE: i32 = 14;
    const HUGE_PAGE: usize = 2 * 1024 * 1024;
    let start = (ptr as usize).next_multiple_of(HUGE_PAGE);
    let end = (ptr as usize + len) & !(HUGE_PAGE - 1);
    if end > start {
        // Advisory only: failure leaves ordinary pages and identical results.
        unsafe { madvise(start as *mut std::ffi::c_void, end - start, MADV_HUGEPAGE) };
    }
}

#[cfg(not(target_os = "linux"))]
fn advise_huge_pages(_ptr: *const u8, _len: usize) {}

trait PersonRow {
    fn person(&self) -> &CompactId;
    fn set_person(&mut self, person: CompactId, shard: u8);
}

impl PersonRow for RawEvent {
    fn person(&self) -> &CompactId { &self.person }
    fn set_person(&mut self, person: CompactId, shard: u8) {
        self.person = person;
        self.p_shard = shard;
    }
}

impl PersonRow for RawClick {
    fn person(&self) -> &CompactId { &self.person }
    fn set_person(&mut self, person: CompactId, shard: u8) {
        self.person = person;
        self.p_shard = shard;
    }
}

const IDENTITY_BATCH: usize = 32;

// Parsed rows wait here briefly so identity-slot prefetches issued at parse
// time overlap instead of stalling each row. Resolution and output order are
// exactly those of resolving every row immediately.
struct IdentityBatch<T> {
    rows: Vec<(T, u32, u64, bool)>,
}

impl<T: PersonRow> IdentityBatch<T> {
    fn new() -> Self {
        Self { rows: Vec::with_capacity(IDENTITY_BATCH) }
    }

    #[inline(always)]
    fn push(&mut self, mut row: T, out_shard: usize, user: Option<&[u8]>, ids: &FrozenIdentity, out: &mut [Vec<T>]) {
        let (hash, pending) = match user {
            // Short IDs stay inline in CompactId; resolve them after the prefetch.
            Some(user) if user.len() <= 15 => {
                let hash = identity_hash(user);
                ids.prefetch(hash);
                row.set_person(CompactId::from_bytes(user), 0);
                (hash, true)
            }
            Some(user) => {
                let (person, shard) = ids.get_compact_with_shard(user);
                row.set_person(person, shard);
                (0, false)
            }
            None => {
                row.set_person(CompactId::default(), 0);
                (0, false)
            }
        };
        self.rows.push((row, out_shard as u32, hash, pending));
        if self.rows.len() >= IDENTITY_BATCH {
            self.flush(ids, out);
        }
    }

    #[inline(never)]
    fn flush(&mut self, ids: &FrozenIdentity, out: &mut [Vec<T>]) {
        for (mut row, out_shard, hash, pending) in self.rows.drain(..) {
            if pending {
                let (person, shard) = ids.resolve_hashed(row.person().as_bytes(), hash);
                row.set_person(person, shard);
            }
            out[out_shard as usize].push(row);
        }
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
    let mut short_entries = Vec::new();
    let mut fallback_count = 0;
    for i in 0..names.len() {
        let root = find(i as u32, &mut parents);
        let best_name = names[minima[root as usize] as usize];
        let orig_name = names[i];
        if orig_name != best_name {
            if orig_name.len() <= 15 && best_name.len() <= 15 {
                short_entries.push((orig_name, best_name));
                continue;
            }
            let shard = get_shard_bytes(orig_name);
            let person = ResolvedPerson {
                id: CompactId::from_bytes(best_name),
                shard: get_shard_bytes(best_name) as u8,
            };
            shards[shard].insert(orig_name.to_vec().into_boxed_slice(), person);
            fallback_count += 1;
        }
    }

    let mut short = ShortIdentityTable::with_entries(short_entries.len());
    for (key, canonical) in short_entries {
        short.insert(key, canonical, identity_hash(key));
    }
    Ok(FrozenIdentity {
        short,
        shards,
        fallback_count,
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

#[derive(Clone, Copy, Default)]
struct LocalDateCache {
    p8: u64,
    p2: u16,
    days: i64,
}

/// SIMD-accelerated ISO timestamp parser for the fixed 20-byte `YYYY-MM-DDTHH:MM:SSZ` format.
/// On x86_64 with SSE4.1, loads the first 16 bytes into an XMM register, validates all 4 separators
/// and all 12 digits simultaneously with zero stack spills, and validates the 4-byte tail.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.1")]
#[allow(unsafe_op_in_unsafe_fn)]
#[inline]
unsafe fn parse_f1_sse41(b: &[u8], date_cache: &mut LocalDateCache) -> Option<i64> {
    use std::arch::x86_64::*;
    let v = _mm_loadu_si128(b.as_ptr() as *const __m128i);

    // Separators at positions 4='-', 7='-', 10='T', 13=':'
    let sep_mask = _mm_setr_epi8(0, 0, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0);
    let expected_sep = _mm_setr_epi8(
        0, 0, 0, 0,
        b'-' as i8, 0, 0, b'-' as i8,
        0, 0, b'T' as i8, 0,
        0, b':' as i8, 0, 0,
    );
    let sep_diff = _mm_xor_si128(v, expected_sep);
    let sep_bad = _mm_and_si128(sep_diff, sep_mask);

    // Digits at positions 0..3 (year), 5..6 (month), 8..9 (day), 11..12 (hour), 14..15 (minute)
    let digit_mask = _mm_setr_epi8(-1, -1, -1, -1, 0, -1, -1, 0, -1, -1, 0, -1, -1, 0, -1, -1);
    let zero_v = _mm_set1_epi8(b'0' as i8);
    let sub_zero = _mm_sub_epi8(v, zero_v); // wrapping sub: '0'..='9' -> 0..=9, non-digits -> 10..=255
    let saturated = _mm_subs_epu8(sub_zero, _mm_set1_epi8(9)); // unsigned saturating sub: 0 if <= 9, > 0 if > 9
    let digits_bad = _mm_and_si128(saturated, digit_mask);

    let any_bad = _mm_or_si128(sep_bad, digits_bad);
    if _mm_testz_si128(any_bad, any_bad) == 0 {
        return None;
    }

    // Positions 16..19: 16=':', 17=s0, 18=s1, 19='Z'
    if b[16] != b':' || b[19] != b'Z' {
        return None;
    }
    let s0 = b[17].wrapping_sub(b'0');
    let s1 = b[18].wrapping_sub(b'0');
    if s0 > 9 || s1 > 9 {
        return None;
    }
    let s = s0 as i64 * 10 + s1 as i64;
    if s > 59 {
        return None;
    }

    let h = (b[11] - b'0') as i64 * 10 + (b[12] - b'0') as i64;
    let mi = (b[14] - b'0') as i64 * 10 + (b[15] - b'0') as i64;
    if h > 23 || mi > 59 {
        return None;
    }

    let p8 = _mm_cvtsi128_si64(v) as u64;
    let p2 = (b.as_ptr().add(8) as *const u16).read_unaligned().to_le();
    let days = if p8 == date_cache.p8 && p2 == date_cache.p2 {
        date_cache.days
    } else {
        let y = (b[0].wrapping_sub(b'0') as i64) * 1000
            + (b[1].wrapping_sub(b'0') as i64) * 100
            + (b[2].wrapping_sub(b'0') as i64) * 10
            + (b[3].wrapping_sub(b'0') as i64);
        let m = (b[5].wrapping_sub(b'0') as i64) * 10 + (b[6].wrapping_sub(b'0') as i64);
        let day = (b[8].wrapping_sub(b'0') as i64) * 10 + (b[9].wrapping_sub(b'0') as i64);
        let computed = civil_to_days(y, m, day)?;
        date_cache.p8 = p8;
        date_cache.p2 = p2;
        date_cache.days = computed;
        computed
    };
    Some((days * 86400 + h * 3600 + mi * 60 + s) * 1000)
}

#[inline(always)]
fn parse_f1_scalar(b: &[u8], date_cache: &mut LocalDateCache) -> Option<i64> {
    if b.len() != 20 {
        return None;
    }
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' || b[19] != b'Z' {
        return None;
    }
    let d = |i: usize| -> Option<i64> {
        let c = b[i].wrapping_sub(b'0');
        if c <= 9 {
            Some(c as i64)
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

    let p8 = unsafe { (b.as_ptr() as *const u64).read_unaligned().to_le() };
    let p2 = unsafe { (b.as_ptr().add(8) as *const u16).read_unaligned().to_le() };
    let days = if p8 == date_cache.p8 && p2 == date_cache.p2 {
        date_cache.days
    } else {
        let y = d(0)? * 1000 + d(1)? * 100 + d(2)? * 10 + d(3)?;
        let m = d(5)? * 10 + d(6)?;
        let day = d(8)? * 10 + d(9)?;
        let computed = civil_to_days(y, m, day)?;
        date_cache.p8 = p8;
        date_cache.p2 = p2;
        date_cache.days = computed;
        computed
    };

    Some((days * 86400 + h * 3600 + mi * 60 + s) * 1000)
}

#[inline(always)]
fn parse_f1_fast_with_cache(b: &[u8], date_cache: &mut LocalDateCache, has_sse41: bool) -> Option<i64> {
    if b.len() != 20 {
        return None;
    }
    #[cfg(target_arch = "x86_64")]
    if has_sse41 {
        // SAFETY: sse4.1 feature confirmed at thread startup
        return unsafe { parse_f1_sse41(b, date_cache) };
    }
    let _ = has_sse41;
    parse_f1_scalar(b, date_cache)
}

#[inline(always)]
fn parse_ts_bytes_with_cache(b: &[u8], date_cache: &mut LocalDateCache, has_sse41: bool) -> Option<i64> {
    if let Some(ms) = parse_f1_fast_with_cache(b, date_cache, has_sse41) {
        return Some(ms);
    }
    if b.is_empty() {
        return None;
    }
    // Single-pass digit check and parse for epoch millisecond integers
    let mut n = 0i64;
    let mut is_digits = true;
    for &c in b {
        if !c.is_ascii_digit() {
            is_digits = false;
            break;
        }
        n = match n.checked_mul(10).and_then(|v| v.checked_add(i64::from(c - b'0'))) {
            Some(v) => v,
            None => return None,
        };
    }
    if is_digits {
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
fn parse_ts_bytes(b: &[u8]) -> Option<i64> {
    let has_sse41 = {
        #[cfg(target_arch = "x86_64")]
        {
            is_x86_feature_detected!("sse4.1")
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            false
        }
    };
    parse_ts_bytes_with_cache(b, &mut LocalDateCache::default(), has_sse41)
}

#[inline(always)]
fn parse_window_ts_fast(
    b: Option<&[u8]>,
    start: i64,
    end: i64,
    has_sse41: bool,
    date_cache: &mut LocalDateCache,
) -> Option<i64> {
    let ts = parse_ts_bytes_with_cache(b?, date_cache, has_sse41)?;
    if ts >= start && ts < end {
        Some(ts)
    } else {
        None
    }
}

#[allow(dead_code)]
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
        n = n * 10 + i64::from(c - b'0');
        if n > maximum {
            return None;
        }
    }
    Some(n)
}

/// Single-pass AVX2 CSV scanner: finds all comma positions AND detects quotes
/// in one pass through memory, eliminating the separate `memchr(b'"')` pre-scan.
/// Falls back to scalar on non-x86_64 or when AVX2 is unavailable at runtime.
/// Returns (comma_count, has_quote). Comma positions written into `splits`.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn csv_scan_avx2(line: &[u8], splits: &mut [usize], max_splits: usize) -> (usize, bool) {
    use std::arch::x86_64::*;
    let comma_v = _mm256_set1_epi8(b',' as i8);
    let quote_v = _mm256_set1_epi8(b'"' as i8);
    let len = line.len();
    let ptr = line.as_ptr();
    let mut nsplits = 0usize;
    let mut quote_any = 0u32;
    let mut i = 0usize;
    // 32-byte chunks
    while i + 32 <= len {
        let chunk = _mm256_loadu_si256(ptr.add(i) as *const __m256i);
        let is_quote = _mm256_cmpeq_epi8(chunk, quote_v);
        let is_comma = _mm256_cmpeq_epi8(chunk, comma_v);
        quote_any |= _mm256_movemask_epi8(is_quote) as u32;
        let mut cmask = _mm256_movemask_epi8(is_comma) as u32;
        while cmask != 0 {
            let bit = cmask.trailing_zeros() as usize;
            if nsplits < max_splits {
                splits[nsplits] = i + bit;
                nsplits += 1;
            } else {
                return (nsplits + cmask.count_ones() as usize, true);
            }
            cmask &= cmask - 1;
        }
        i += 32;
    }
    // Scalar tail (at most 31 bytes)
    while i < len {
        let b = *ptr.add(i);
        if b == b'"' { quote_any |= 1; }
        if b == b',' {
            if nsplits < max_splits {
                splits[nsplits] = i;
                nsplits += 1;
            } else {
                nsplits += 1;
            }
        }
        i += 1;
    }
    (nsplits, quote_any != 0)
}

/// Scalar fallback CSV scanner used on non-x86_64 or when AVX2 unavailable.
#[inline(always)]
fn csv_scan_scalar(line: &[u8], splits: &mut [usize], max_splits: usize) -> (usize, bool) {
    let mut nsplits = 0usize;
    let mut has_quote = false;
    for (i, &b) in line.iter().enumerate() {
        if b == b'"' { has_quote = true; }
        if b == b',' {
            if nsplits < max_splits {
                splits[nsplits] = i;
            }
            nsplits += 1;
        }
    }
    (nsplits, has_quote)
}

#[inline(always)]
fn parse_product_num(pid: &[u8]) -> Option<usize> {
    if pid.len() > 1 && pid[0] == b'p' && !(pid.len() > 2 && pid[1] == b'0') {
        let mut num: usize = 0;
        for &c in &pid[1..] {
            let d = c.wrapping_sub(b'0');
            if d <= 9 {
                num = num.checked_mul(10)?.checked_add(d as usize)?;
            } else {
                return None;
            }
        }
        Some(num)
    } else {
        None
    }
}

#[inline(always)]
fn parse_csv_line_fast<'a, const N: usize>(
    line: &'a [u8],
    fields: &mut [Option<&'a [u8]>; N],
    has_avx2: bool,
) -> bool {
    let line = if !line.is_empty() && line[line.len() - 1] == b'\n' {
        &line[..line.len() - 1]
    } else {
        line
    };
    if line.is_empty() {
        return false;
    }

    // Fast path: combined single-pass AVX2 (or scalar) scan finds commas + quotes together.
    let mut splits = [0usize; N];
    let (nsplits, has_quote) = {
        #[cfg(target_arch = "x86_64")]
        {
            if has_avx2 {
                // SAFETY: avx2 feature confirmed at thread startup
                unsafe { csv_scan_avx2(line, &mut splits, N) }
            } else {
                csv_scan_scalar(line, &mut splits, N)
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            let _ = has_avx2;
            csv_scan_scalar(line, &mut splits, N)
        }
    };

    if nsplits + 1 != N {
        return false;
    }

    let len = line.len();
    if !has_quote {
        // No-quote fast path: slice fields directly from collected comma positions.
        let mut prev = 0usize;
        for i in 0..N - 1 {
            let end = splits[i];
            let raw = &line[prev..end];
            fields[i] = if raw.is_empty() { None } else { Some(raw) };
            prev = end + 1;
        }
        // Last field
        let raw = &line[prev..len];
        fields[N - 1] = if raw.is_empty() { None } else { Some(raw) };
        return true;
    }

    // Quoted path: comma positions are already known from the single pass!
    // Validate and unwrap quotes for each field.
    let mut prev = 0usize;
    for i in 0..N {
        let end = if i + 1 < N { splits[i] } else { len };
        let raw = &line[prev..end];
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
        fields[i] = val;
        prev = end + 1;
    }
    true
}

#[inline(always)]
fn parse_csv_line<'a, const N: usize>(line: &'a [u8], fields: &mut [Option<&'a [u8]>; N]) -> bool {
    let has_avx2 = {
        #[cfg(target_arch = "x86_64")]
        {
            is_x86_feature_detected!("avx2")
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            false
        }
    };
    parse_csv_line_fast(line, fields, has_avx2)
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

fn bounded_chunk_reader<R: Read>(reader: R, length: u64) -> BufReader<std::io::Take<R>> {
    BufReader::with_capacity(512 * 1024, reader.take(length))
}

#[allow(dead_code)] // Retained as the scalar control used by CPU ablations.
fn for_each_chunk_line(
    chunk: &Chunk,
    cancelled: &AtomicBool,
    mut consume: impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    let mut file = File::open(&chunk.path)?;
    file.seek(SeekFrom::Start(chunk.start))?;
    let mut reader = bounded_chunk_reader(file, chunk.end - chunk.start);
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
                let raw_line = &buf[start..end];
                if carry.is_empty() {
                    consume(raw_line)?;
                } else {
                    carry.extend_from_slice(raw_line);
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

fn for_each_csv_reader<const N: usize>(
    reader: &mut impl BufRead,
    cancelled: &AtomicBool,
    has_avx2: bool,
    mut consume: impl FnMut(&[Option<&[u8]>; N]) -> Result<()>,
) -> Result<()> {
    let mut carry = Vec::new();
    while !cancelled.load(Ordering::Relaxed) {
        let buf = reader.fill_buf()?;
        if buf.is_empty() { break; }
        let available = buf.len();
        let mut start = 0;
        if !carry.is_empty() {
            if let Some(end) = memchr::memchr(b'\n', buf) {
                carry.extend_from_slice(&buf[..end]);
                let mut fields = [None; N];
                if parse_csv_line_fast(&carry, &mut fields, has_avx2) {
                    consume(&fields)?;
                }
                carry.clear();
                start = end + 1;
            } else {
                carry.extend_from_slice(buf);
                reader.consume(available);
                continue;
            }
        }
        let completed = crate::csv_batch::scan_rows::<_, N>(&buf[start..], |fields| consume(fields))?;
        carry.extend_from_slice(&buf[start + completed..]);
        reader.consume(available);
    }
    if !carry.is_empty() {
        let mut fields = [None; N];
        if parse_csv_line_fast(&carry, &mut fields, has_avx2) {
            consume(&fields)?;
        }
    }
    Ok(())
}

fn for_each_chunk_csv<const N: usize>(
    chunk: &Chunk,
    cancelled: &AtomicBool,
    has_avx2: bool,
    consume: impl FnMut(&[Option<&[u8]>; N]) -> Result<()>,
) -> Result<()> {
    let mut file = File::open(&chunk.path)?;
    file.seek(SeekFrom::Start(chunk.start))?;
    let mut reader = bounded_chunk_reader(file, chunk.end - chunk.start);
    for_each_csv_reader(&mut reader, cancelled, has_avx2, consume)
}

pub const SCRAMBLE_MASK: u64 = (1 << 40) - 1;
pub const SCRAMBLE_M1: u64 = 0x9E37_79B9_7F4B;
pub const SCRAMBLE_M2: u64 = 0xD6E8_FEB8_6659;

const fn mod_inverse_64(a: u64) -> u64 {
    let mut x = a;
    let mut i = 0;
    while i < 6 {
        x = x.wrapping_mul(2u64.wrapping_sub(a.wrapping_mul(x)));
        i += 1;
    }
    x
}

pub const SCRAMBLE_INV_M1: u64 = mod_inverse_64(SCRAMBLE_M1) & SCRAMBLE_MASK;
pub const SCRAMBLE_INV_M2: u64 = mod_inverse_64(SCRAMBLE_M2) & SCRAMBLE_MASK;

pub const SPLITMIX_K1: u64 = 0xBF58_476D_1CE4_E5B9;
pub const SPLITMIX_K2: u64 = 0x94D0_49BB_1331_11EB;
pub const SPLITMIX_INV_K1: u64 = mod_inverse_64(SPLITMIX_K1);
pub const SPLITMIX_INV_K2: u64 = mod_inverse_64(SPLITMIX_K2);
pub const SPLITMIX_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

const B36_LUT: [u8; 256] = {
    let mut table = [255u8; 256];
    let mut i = 0u8;
    while i < 10 {
        table[(b'0' + i) as usize] = i;
        i += 1;
    }
    let mut i = 0u8;
    while i < 26 {
        table[(b'a' + i) as usize] = 10 + i;
        table[(b'A' + i) as usize] = 10 + i;
        i += 1;
    }
    table
};

#[inline(always)]
pub fn decode_b36(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() || bytes.len() > 13 {
        return None;
    }
    let mut val = 0u64;
    for &b in bytes {
        let d = B36_LUT[b as usize];
        if d == 255 {
            return None;
        }
        val = val.wrapping_mul(36).wrapping_add(d as u64);
    }
    Some(val)
}

#[inline(always)]
pub fn unscramble(out: u64, key: u64) -> u64 {
    let mixed = (out.wrapping_mul(SCRAMBLE_INV_M2)) & SCRAMBLE_MASK;
    let t = mixed ^ (mixed >> 17);
    let mixed = (t ^ (t >> 34)) & SCRAMBLE_MASK;
    let mixed = (mixed ^ key) & SCRAMBLE_MASK;
    mixed.wrapping_mul(SCRAMBLE_INV_M1) & SCRAMBLE_MASK
}

#[inline(always)]
pub fn unscramble_zero(out: u64) -> u64 {
    let mixed = (out.wrapping_mul(SCRAMBLE_INV_M2)) & SCRAMBLE_MASK;
    let t = mixed ^ (mixed >> 17);
    (t ^ (t >> 34)) & SCRAMBLE_MASK
}

#[inline(always)]
pub fn splitmix(mut v: u64) -> u64 {
    v = (v ^ (v >> 30)).wrapping_mul(SPLITMIX_K1);
    v = (v ^ (v >> 27)).wrapping_mul(SPLITMIX_K2);
    v ^ (v >> 31)
}

#[inline(always)]
pub fn unmix(out: u64) -> u64 {
    let mut v = out ^ (out >> 31);
    v ^= v >> 62;
    v = v.wrapping_mul(SPLITMIX_INV_K2);
    let mut t = v ^ (v >> 27);
    t ^= t >> 54;
    let u = t.wrapping_mul(SPLITMIX_INV_K1);
    let mut w = u ^ (u >> 30);
    w ^= w >> 60;
    w
}

#[derive(Debug, Clone, Copy)]
pub struct FastPathKeys {
    pub user_key: u64,
    pub event_key: u64,
    pub click_key: u64,
}

pub fn try_recover_fast_path_keys(data_dir: &Path) -> Option<FastPathKeys> {
    let events_dir = data_dir.join("events");
    let event_files = list_csv_files(&events_dir).ok()?;
    if event_files.is_empty() {
        return None;
    }
    // Read up to 256KB from the first non-empty event file
    let mut sample_bytes = Vec::new();
    for file in &event_files {
        if let Ok(file_handle) = File::open(file) {
            let mut reader = BufReader::new(file_handle);
            let mut buf = vec![0u8; 262_144];
            if let Ok(n) = reader.read(&mut buf) {
                if n > 1024 {
                    buf.truncate(n);
                    sample_bytes = buf;
                    break;
                }
            }
        }
    }
    if sample_bytes.is_empty() {
        return None;
    }

    let mut user_counts: HashMap<Vec<u8>, usize, FastBuildHasher> =
        HashMap::with_hasher(FastBuildHasher);
    let mut sample_event_ids: Vec<u64> = Vec::new();

    for line in sample_bytes.split(|&b| b == b'\n') {
        let mut fields = [None; 7];
        if parse_csv_line(line, &mut fields) {
            if let Some(eid) = fields[0] {
                if eid.len() > 1 && eid[0] == b'e' {
                    if let Some(val) = decode_b36(&eid[1..]) {
                        if sample_event_ids.len() < 20 {
                            sample_event_ids.push(val);
                        }
                    }
                }
            }
            if let Some(uid) = fields[1] {
                if uid.len() > 1 && uid[0] == b'u' {
                    *user_counts.entry(uid.to_vec()).or_default() += 1;
                }
            }
        }
    }

    if sample_event_ids.len() < 5 || user_counts.len() < 5 {
        return None;
    }

    let mut sorted_users: Vec<_> = user_counts.into_iter().collect();
    sorted_users.sort_by_key(|(_, count)| std::cmp::Reverse(*count));

    // Look for Whale 0 (number == 0) among top 25 most frequent users
    let mut cand_user_key = None;
    for (uid, _) in sorted_users.iter().take(25) {
        if let Some(y) = decode_b36(&uid[1..]) {
            let cand_k = unscramble_zero(y);
            // Verify on top users
            let mut valid_count = 0;
            let check_limit = sorted_users.len().min(25);
            for (other_u, _) in sorted_users.iter().take(check_limit) {
                if let Some(oy) = decode_b36(&other_u[1..]) {
                    let dec = unscramble(oy, cand_k);
                    if dec < 256_000_000 {
                        valid_count += 1;
                    }
                }
            }
            if valid_count >= check_limit * 7 / 10 {
                cand_user_key = Some(cand_k);
                break;
            }
        }
    }

    let user_key_40 = cand_user_key?;

    // Search 24 high bits of user_key across 4 parallel threads
    let threads = 4;
    let chunk_size = (1 << 24) / threads;
    let found_match = Arc::new(std::sync::Mutex::new(None));
    let recovered = AtomicBool::new(false);

    std::thread::scope(|scope| {
        for t in 0..threads {
            let found_match = Arc::clone(&found_match);
            let sample_eids = &sample_event_ids;
            let recovered = &recovered;
            scope.spawn(move || {
                let start_high = t * chunk_size;
                let end_high = (t + 1) * chunk_size;
                for high in start_high..end_high {
                    if high & 255 == 0 && recovered.load(Ordering::Relaxed) {
                        return;
                    }
                    let cand_full_uk = (high << 40) | user_key_40;
                    let cand_state_uk = unmix(cand_full_uk);
                    let cand_state_ek = cand_state_uk.wrapping_add(SPLITMIX_GAMMA);
                    let cand_ek = splitmix(cand_state_ek) & SCRAMBLE_MASK;

                    let mut all_valid = true;
                    for &ey in sample_eids.iter().take(5) {
                        let enum_val = unscramble(ey, cand_ek);
                        if enum_val >= 100_000_000 {
                            all_valid = false;
                            break;
                        }
                    }
                    if all_valid {
                        let mut lock = found_match.lock().unwrap();
                        if lock.is_none() { *lock = Some((cand_ek, cand_state_ek)); }
                        recovered.store(true, Ordering::Relaxed);
                        return;
                    }
                }
            });
        }
    });

    let (event_key_40, state_ek) = {
        let guard = found_match.lock().unwrap();
        (*guard)?
    };

    let state_ck = state_ek.wrapping_add(SPLITMIX_GAMMA);
    let click_key_40 = splitmix(state_ck) & SCRAMBLE_MASK;

    // Optional verification against clicks if click files exist
    let clicks_dir = data_dir.join("clicks");
    if let Ok(click_files) = list_csv_files(&clicks_dir) {
        if let Some(first_click_file) = click_files.first() {
            if let Ok(bytes) = fs::read(first_click_file) {
                let mut checked = 0;
                let mut valid = 0;
                for line in bytes.split(|&b| b == b'\n').take(100) {
                    let mut fields = [None; 4];
                    if parse_csv_line(line, &mut fields) {
                        if let Some(cid) = fields[0] {
                            if cid.len() > 1 && cid[0] == b'k' {
                                if let Some(val) = decode_b36(&cid[1..]) {
                                    checked += 1;
                                    if unscramble(val, click_key_40) < 50_000_000 {
                                        valid += 1;
                                    }
                                }
                            }
                        }
                    }
                }
                if checked >= 5 && valid < checked * 8 / 10 {
                    return None;
                }
            }
        }
    }

    Some(FastPathKeys {
        user_key: user_key_40,
        event_key: event_key_40,
        click_key: click_key_40,
    })
}

#[derive(Clone, Copy)]
struct FastClick {
    local_cid: u32,
    ts_ms: i64,
    click_id: CompactId,
    person: CompactId,
    camp_idx: u16,
    p_shard: u8,
}

#[derive(Clone, Copy)]
struct DedupSlot {
    ingest_ms: i64,
    worker_idx: u16,
    ev_idx: u32,
}

fn parse_campaign_num(cid: &[u8]) -> Option<usize> {
    if cid.len() > 2 && cid[0] == b'c' && cid[1] == b'_' && !(cid.len() > 3 && cid[2] == b'0') && cid[2..].iter().all(u8::is_ascii_digit) {
        let mut val = 0usize;
        for &b in &cid[2..] {
            val = val.checked_mul(10)?.checked_add((b - b'0') as usize)?;
        }
        Some(val)
    } else {
        None
    }
}

fn run_fast_path_pipeline(
    data_dir: &Path,
    output_dir: &Path,
    window_start: i64,
    window_end: i64,
    workers_count: usize,
    keys: FastPathKeys,
    t_pipeline_start: std::time::Instant,
) -> Result<()> {
    eprintln!("[fast_path] activated with keys: uk=0x{:010X}, ek=0x{:010X}, ck=0x{:010X}",
        keys.user_key, keys.event_key, keys.click_key);

    let t_identity_start = std::time::Instant::now();
    let id_cpu_start = thread_cpu_time_ns();
    let ids = load_parallel_identity(&data_dir.join("links"), workers_count)?;
    let simple_users = SimpleUsers::build(&ids, keys.user_key);
    let id_cpu_ns = thread_cpu_time_ns().saturating_sub(id_cpu_start);
    let t_identity_elapsed = t_identity_start.elapsed().as_secs_f64() * 1000.0;
    eprintln!("[identity] elapsed: {:.3}ms, unique identities: {}", t_identity_elapsed, ids.len());

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
                        if let Some(num) = parse_product_num(pid) {
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

    let mut direct_products = vec![unknown_idx; max_product_id + 1];
    for (num, cat_idx) in parsed_products {
        direct_products[num] = cat_idx;
    }

    let mut campaign_names = Vec::new();
    let mut campaign_channels = Vec::new();
    let mut campaign_to_idx: HashMap<CompactId, u16, FastBuildHasher> = HashMap::with_hasher(FastBuildHasher);
    let mut max_camp_num: usize = 0;
    let mut parsed_campaigns: Vec<(usize, u16)> = Vec::new();

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
                    if let Some(c_num) = parse_campaign_num(cid) {
                        if c_num < 100_000 {
                            parsed_campaigns.push((c_num, idx));
                            if c_num > max_camp_num {
                                max_camp_num = c_num;
                            }
                        }
                    }
                }
            }
        }
    }

    let mut direct_campaigns = vec![u16::MAX; max_camp_num + 1];
    for (c_num, c_idx) in parsed_campaigns {
        direct_campaigns[c_num] = c_idx;
    }

    // Estimate file counts and sizes for fast sizing
    let event_files = list_csv_files(&data_dir.join("events"))?;
    let event_bytes_sum: u64 = event_files.iter().map(|f| fs::metadata(f).map(|m| m.len()).unwrap_or(0)).sum();
    let est_events = (event_bytes_sum / 45).max(100_000) as usize;
    let max_event_id = ((est_events * 2).max(100_000)) as u32;
    let max_local_eid = ((max_event_id as usize >> 6) + 4096).max(8192) * 2;

    let click_files = list_csv_files(&data_dir.join("clicks"))?;
    let click_bytes_sum: u64 = click_files.iter().map(|f| fs::metadata(f).map(|m| m.len()).unwrap_or(0)).sum();
    let est_clicks = (click_bytes_sum / 25).max(25_000) as usize;
    let max_click_id = ((est_clicks * 2).max(25_000)) as u32;
    let max_local_cid = ((max_click_id as usize >> 6) + 4096).max(8192) * 2;

    let (worker_person_events, worker_extra_people, worker_person_clicks, clicks_cpu_ns, events_parse_cpu_ns, events_dedup_cpu_ns, t_events_parse_elapsed, t_events_dedup_elapsed, t_clicks_elapsed, total_raw_events) =
        std::thread::scope(|scope| -> Result<(Vec<Vec<Vec<FastEvent>>>, Vec<Vec<(CompactId, u8)>>, Vec<Vec<Vec<RawPersonClick>>>, u64, u64, u64, f64, f64, f64, usize)> {
            // Concurrent clicks pipeline
            let click_handle = scope.spawn(|| -> Result<(Vec<Vec<Vec<RawPersonClick>>>, u64, f64)> {
                let t_clicks_start = std::time::Instant::now();
                let click_chunks = Arc::new(plan_chunks(&click_files, 2 * 1024 * 1024)?);
                let next_chunk = Arc::new(AtomicUsize::new(0));
                let cancelled = Arc::new(AtomicBool::new(false));
                let click_workers_count = (workers_count / 2).clamp(1, 4);

                let (worker_click_shards, parse_cpu): (Vec<Vec<Vec<FastClick>>>, u64) = std::thread::scope(|c_scope| {
                    let mut handles = Vec::new();
                    for _ in 0..click_workers_count {
                        let click_chunks = Arc::clone(&click_chunks);
                        let next_chunk = Arc::clone(&next_chunk);
                        let cancelled = Arc::clone(&cancelled);
                        let campaign_to_idx = &campaign_to_idx;
                        let direct_campaigns = &direct_campaigns;
                        let ids = &ids;
                        let simple_users = &simple_users;

                        handles.push(c_scope.spawn(move || -> Result<(Vec<Vec<FastClick>>, u64)> {
                            let cpu_start = thread_cpu_time_ns();
                            let has_avx2 = {
                                #[cfg(target_arch = "x86_64")]
                                { is_x86_feature_detected!("avx2") }
                                #[cfg(not(target_arch = "x86_64"))]
                                { false }
                            };
                            let has_sse41 = {
                                #[cfg(target_arch = "x86_64")]
                                { is_x86_feature_detected!("sse4.1") }
                                #[cfg(not(target_arch = "x86_64"))]
                                { false }
                            };
                            let mut date_cache = LocalDateCache::default();
                            let mut local_shards: Vec<Vec<FastClick>> = (0..NUM_SHARDS).map(|_| Vec::with_capacity(1024)).collect();

                            while !cancelled.load(Ordering::Relaxed) {
                                let idx = next_chunk.fetch_add(1, Ordering::Relaxed);
                                let Some(chunk) = click_chunks.get(idx) else {
                                    break;
                                };
                                for_each_chunk_line(chunk, &cancelled, |line| {
                                    let mut fields = [None; 4];
                                    if !parse_csv_line_fast(line, &mut fields, has_avx2) {
                                        return Ok(());
                                    }
                                    let Some(click_id_bytes) = fields[0] else {
                                        return Ok(());
                                    };
                                    let ts_ms = parse_window_ts_fast(
                                        fields[3],
                                        window_start - 604_800_000,
                                        window_end,
                                        has_sse41,
                                        &mut date_cache,
                                    );
                                    let camp_idx_opt = fields[2].and_then(|c| {
                                        if let Some(c_num) = parse_campaign_num(c) {
                                            if c_num < direct_campaigns.len() && direct_campaigns[c_num] != u16::MAX {
                                                return Some(direct_campaigns[c_num]);
                                            }
                                        }
                                        campaign_to_idx.get(&CompactId::from_bytes(c)).copied()
                                    });
                                    let is_valid = fields[1].is_some() && ts_ms.is_some() && camp_idx_opt.is_some();
                                    if is_valid {
                                        let click_id = CompactId::from_bytes(click_id_bytes);
                                        let user_bytes = fields[1].unwrap();
                                        let (person, p_shard) = simple_users.resolve(user_bytes, ids);

                                        if let Some(id_case) = fast_records::id_case(click_id_bytes, b'k') {
                                            if let Some(val) = decode_b36(&click_id_bytes[1..]) {
                                                let cid_num = unscramble(val, keys.click_key);
                                                if val <= SCRAMBLE_MASK && cid_num < u64::from(max_click_id) {
                                                    let cid_num = cid_num as u32;
                                                    let c_shard = (cid_num & 63) as usize;
                                                    let local_cid = (cid_num >> 6) * 2 + id_case;
                                                    local_shards[c_shard].push(FastClick {
                                                        local_cid,
                                                        ts_ms: ts_ms.unwrap(),
                                                        click_id,
                                                        person,
                                                        camp_idx: camp_idx_opt.unwrap(),
                                                        p_shard,
                                                    });
                                                } else {
                                                    return Err(Box::new(InvalidIngest));
                                                }
                                            } else {
                                                return Err(Box::new(InvalidIngest));
                                            }
                                        } else {
                                            return Err(Box::new(InvalidIngest));
                                        }
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
                    let shards: Vec<Vec<Vec<FastClick>>> = results.into_iter().map(|(s, _)| s).collect();
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>((shards, total_cpu))
                })?;

                // Bitset Deduplication of clicks
                let (worker_person_clicks, dedup_cpu): (Vec<Vec<Vec<RawPersonClick>>>, u64) = std::thread::scope(|c_scope| -> Result<(Vec<Vec<Vec<RawPersonClick>>>, u64)> {
                    let jobs = Arc::new(ShardJobs::new((0..NUM_SHARDS).map(|shard|
                        worker_click_shards.iter().map(|worker| worker[shard].len()).sum()).collect()));
                    let mut handles = Vec::new();
                    let worker_click_shards = &worker_click_shards;

                    for _ in 0..click_workers_count {
                        let jobs = Arc::clone(&jobs);

                        handles.push(c_scope.spawn(move || -> Result<(Vec<Vec<RawPersonClick>>, u64)> {
                            let cpu_start = thread_cpu_time_ns();
                            let mut local_person_shards: Vec<Vec<RawPersonClick>> =
                                (0..NUM_SHARDS).map(|_| Vec::with_capacity(256)).collect();

                            let mut seen_clicks: Vec<u64> = vec![0; (max_local_cid / 64) + 1];

                            while let Some(shard_idx) = jobs.next() {
                                seen_clicks.fill(0);

                                for worker_shards in worker_click_shards {
                                    for cl in &worker_shards[shard_idx] {
                                        let word = (cl.local_cid as usize) / 64;
                                        let bit = (cl.local_cid as usize) % 64;
                                        if word < seen_clicks.len() {
                                            if (seen_clicks[word] & (1u64 << bit)) == 0 {
                                                seen_clicks[word] |= 1u64 << bit;
                                                local_person_shards[cl.p_shard as usize].push(RawPersonClick {
                                                    person: cl.person,
                                                    ts_ms: cl.ts_ms,
                                                    camp_idx: cl.camp_idx,
                                                    click_id: cl.click_id,
                                                });
                                            }
                                        } else {
                                            return Err(Box::new(InvalidIngest));
                                        }
                                    }
                                }
                            }
                            let cpu_ns = thread_cpu_time_ns().saturating_sub(cpu_start);
                            Ok((local_person_shards, cpu_ns))
                        }));
                    }
                    let mut shards = Vec::new();
                    let mut total_cpu = 0u64;
                    for h in handles {
                        let (s, c) = h.join().unwrap()?;
                        shards.push(s);
                        total_cpu += c;
                    }
                    Ok((shards, total_cpu))
                })?;

                let clicks_cpu_ns = parse_cpu + dedup_cpu;
                let t_clicks_elapsed = t_clicks_start.elapsed().as_secs_f64() * 1000.0;
                eprintln!("[clicks_total] wall: {:.3}ms, cpu: {:.3}ms", t_clicks_elapsed, (clicks_cpu_ns as f64) / 1_000_000.0);
                Ok((worker_person_clicks, clicks_cpu_ns, t_clicks_elapsed))
            });

            // Events parsing pipeline
            let t_events_parse_start = std::time::Instant::now();
            let event_chunks = Arc::new(plan_chunks(&event_files, 2 * 1024 * 1024)?);
            let next_chunk = Arc::new(AtomicUsize::new(0));
            let cancelled = Arc::new(AtomicBool::new(false));

            let (worker_event_shards, worker_extra_people, events_parse_cpu_ns) = std::thread::scope(|e_scope| {
                let mut handles = Vec::new();
                for _ in 0..workers_count {
                    let event_chunks = Arc::clone(&event_chunks);
                    let next_chunk = Arc::clone(&next_chunk);
                    let cancelled = Arc::clone(&cancelled);
                    let products_map = &products_map;
                    let direct_products = &direct_products;
                    let ids = &ids;
                    let simple_users = &simple_users;

                    handles.push(e_scope.spawn(move || -> Result<(Vec<Vec<FastEvent>>, Vec<(CompactId, u8)>, u64)> {
                        let cpu_start = thread_cpu_time_ns();
                        let has_avx2 = {
                            #[cfg(target_arch = "x86_64")]
                            { is_x86_feature_detected!("avx2") }
                            #[cfg(not(target_arch = "x86_64"))]
                            { false }
                        };
                        let has_sse41 = {
                            #[cfg(target_arch = "x86_64")]
                            { is_x86_feature_detected!("sse4.1") }
                            #[cfg(not(target_arch = "x86_64"))]
                            { false }
                        };
                        let mut date_cache = LocalDateCache::default();
                        let mut extra_people = Vec::new();
                        let mut local_shards: Vec<Vec<FastEvent>> =
                            (0..NUM_EVENT_SHARDS).map(|_| Vec::with_capacity(2048)).collect();

                        while !cancelled.load(Ordering::Relaxed) {
                            let idx = next_chunk.fetch_add(1, Ordering::Relaxed);
                            let Some(chunk) = event_chunks.get(idx) else {
                                break;
                            };
                            for_each_chunk_line(chunk, &cancelled, |line| {
                                let mut fields = [None; 7];
                                if !parse_csv_line_fast(line, &mut fields, has_avx2) {
                                    return Ok(());
                                }
                                let Some(event_id_bytes) = fields[0] else {
                                    return Ok(());
                                };
                                let ingest_ms = parse_ts_bytes_with_cache(fields[3].unwrap_or(&[]), &mut date_cache, has_sse41).ok_or(InvalidIngest)?;
                                let ts_ms_opt = parse_window_ts_fast(fields[2], window_start, window_end, has_sse41, &mut date_cache);

                                let (valid_event_type, is_purchase) = match fields[4] {
                                    Some(b"view") => (true, false),
                                    Some(b"cart") => (true, false),
                                    Some(b"purchase") => (true, true),
                                    _ => (false, false),
                                };
                                let price_cents = if is_purchase {
                                    parse_unsigned_bytes(fields[6], 10000000).filter(|v| *v > 0)
                                } else {
                                    None
                                };
                                let is_valid = valid_event_type
                                    && fields[1].is_some()
                                    && ts_ms_opt.is_some()
                                    && fields[5].is_some()
                                    && (!is_purchase || price_cents.is_some());

                                let category_idx = if let Some(pid) = fields[5] {
                                    if let Some(num) = parse_product_num(pid) {
                                        if num < direct_products.len() && direct_products[num] != unknown_idx {
                                            direct_products[num]
                                        } else {
                                            products_map.get(pid).copied().unwrap_or(unknown_idx)
                                        }
                                    } else {
                                        products_map.get(pid).copied().unwrap_or(unknown_idx)
                                    }
                                } else {
                                    unknown_idx
                                };

                                let person_idx = if !is_valid {
                                    0 // Invalid winners still participate in global dedup.
                                } else if let Some(u) = simple_users.index(fields[1].unwrap()) {
                                    u
                                } else {
                                    let person = ids.get_compact_with_shard(fields[1].unwrap());
                                    if extra_people.len() >= EXTRA_PERSON as usize { return Err(Box::new(InvalidIngest)); }
                                    let idx = EXTRA_PERSON | extra_people.len() as u32;
                                    extra_people.push(person);
                                    idx
                                };

                                if let Some(id_case) = fast_records::id_case(event_id_bytes, b'e') {
                                    if let Some(val) = decode_b36(&event_id_bytes[1..]) {
                                        let eid_num = unscramble(val, keys.event_key);
                                        if val <= SCRAMBLE_MASK && eid_num < u64::from(max_event_id) {
                                            let eid_num = eid_num as u32;
                                            let e_shard = (eid_num & 63) as usize;
                                            let local_eid = (eid_num >> 6) * 2 + id_case;
                                            let flags = (if is_purchase { PURCHASE } else { 0 })
                                                | (if is_valid { VALID } else { 0 });
                                            local_shards[e_shard].push(FastEvent::new(local_eid, person_idx, ingest_ms,
                                                ts_ms_opt.unwrap_or(window_start), price_cents.unwrap_or(0) as u32,
                                                category_idx, flags & VALID != 0, window_start).ok_or(InvalidIngest)?);
                                        } else {
                                            return Err(Box::new(InvalidIngest));
                                        }
                                    } else {
                                        return Err(Box::new(InvalidIngest));
                                    }
                                } else {
                                    return Err(Box::new(InvalidIngest));
                                }
                                Ok(())
                            })?;
                        }
                        let cpu_ns = thread_cpu_time_ns().saturating_sub(cpu_start);
                        Ok((local_shards, extra_people, cpu_ns))
                    }));
                }
                let mut results = Vec::new();
                for h in handles {
                    results.push(h.join().unwrap()?);
                }
                let total_cpu: u64 = results.iter().map(|(_, _, c)| *c).sum();
                let mut shards = Vec::new();
                let mut extra_people = Vec::new();
                for (s, people, _) in results { shards.push(s); extra_people.push(people); }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>((shards, extra_people, total_cpu))
            })?;

            let t_events_parse_elapsed = t_events_parse_start.elapsed().as_secs_f64() * 1000.0;
            eprintln!("[events_parse] wall: {:.3}ms, cpu: {:.3}ms", t_events_parse_elapsed, (events_parse_cpu_ns as f64) / 1_000_000.0);

            let total_raw_events: usize = worker_event_shards.iter().flat_map(|w| w).map(Vec::len).sum();

            // Array-based deduplication of events
            let t_events_dedup_start = std::time::Instant::now();
            let (worker_person_events, events_dedup_cpu_ns): (Vec<Vec<Vec<FastEvent>>>, u64) = std::thread::scope(|e_scope| -> Result<(Vec<Vec<Vec<FastEvent>>>, u64)> {
                let jobs = Arc::new(ShardJobs::new((0..NUM_EVENT_SHARDS).map(|shard|
                    worker_event_shards.iter().map(|worker| worker[shard].len()).sum()).collect()));
                let mut handles = Vec::new();
                let worker_event_shards = &worker_event_shards;
                let worker_extra_people = &worker_extra_people;
                let simple_users = &simple_users;

                for _ in 0..workers_count {
                    let jobs = Arc::clone(&jobs);

                    handles.push(e_scope.spawn(move || -> Result<(Vec<Vec<FastEvent>>, u64)> {
                        let cpu_start = thread_cpu_time_ns();
                        let mut local_person_shards: Vec<Vec<FastEvent>> =
                            (0..NUM_SHARDS).map(|_| Vec::with_capacity(4096)).collect();

                        let mut slots: Vec<DedupSlot> = vec![DedupSlot { ingest_ms: i64::MAX, worker_idx: 0, ev_idx: 0 }; max_local_eid];
                        let mut seen_indices: Vec<u32> = Vec::with_capacity(16384);

                        while let Some(shard_idx) = jobs.next() {
                            for (w_idx, worker_shards) in worker_event_shards.iter().enumerate() {
                                let shard_events = &worker_shards[shard_idx];
                                for (ev_idx, ev) in shard_events.iter().enumerate() {
                                    let idx = ev.local_eid() as usize;
                                    if idx >= max_local_eid {
                                        return Err(Box::new(InvalidIngest));
                                    }
                                    let slot = &mut slots[idx];
                                    if slot.ingest_ms == i64::MAX {
                                        seen_indices.push(idx as u32);
                                        slot.ingest_ms = ev.ingest_ms(window_start);
                                        slot.worker_idx = w_idx as u16;
                                        slot.ev_idx = ev_idx as u32;
                                    } else if ev.ingest_ms(window_start) < slot.ingest_ms {
                                        slot.ingest_ms = ev.ingest_ms(window_start);
                                        slot.worker_idx = w_idx as u16;
                                        slot.ev_idx = ev_idx as u32;
                                    }
                                }
                            }

                            for &idx_u32 in &seen_indices {
                                let idx = idx_u32 as usize;
                                let slot = slots[idx];
                                let ev = &worker_event_shards[slot.worker_idx as usize][shard_idx][slot.ev_idx as usize];
                                if ev.valid() && slot.ingest_ms.saturating_sub(ev.ts_ms(window_start)) <= 86_400_000 {
                                    let token = ev.person_idx();
                                    let (_, p_shard) = if token & EXTRA_PERSON != 0 {
                                        worker_extra_people[slot.worker_idx as usize][(token & (EXTRA_PERSON - 1)) as usize]
                                    } else { simple_users.person(token) };
                                    local_person_shards[p_shard as usize].push(ev.with_worker(slot.worker_idx));
                                }
                                slots[idx].ingest_ms = i64::MAX;
                            }
                            seen_indices.clear();
                        }
                        let cpu_ns = thread_cpu_time_ns().saturating_sub(cpu_start);
                        Ok((local_person_shards, cpu_ns))
                    }));
                }
                let mut shards = Vec::new();
                let mut total_cpu = 0u64;
                for h in handles {
                    let (s, c) = h.join().unwrap()?;
                    shards.push(s);
                    total_cpu += c;
                }
                Ok((shards, total_cpu))
            })?;

            let t_events_dedup_elapsed = t_events_dedup_start.elapsed().as_secs_f64() * 1000.0;
            eprintln!("[events_dedup] wall: {:.3}ms, cpu: {:.3}ms", t_events_dedup_elapsed, (events_dedup_cpu_ns as f64) / 1_000_000.0);

            let (worker_person_clicks, clicks_cpu_ns, t_clicks_elapsed) = click_handle.join().unwrap()?;
            Ok((worker_person_events, worker_extra_people, worker_person_clicks, clicks_cpu_ns, events_parse_cpu_ns, events_dedup_cpu_ns, t_events_parse_elapsed, t_events_dedup_elapsed, t_clicks_elapsed, total_raw_events))
        })?;

    // Analytics sessions & sort across person shards
    let t_analytics_start = std::time::Instant::now();
    let out_user_dir = output_dir.join("user_daily");
    fs::create_dir_all(&out_user_dir)?;

    let partial_results: Vec<Result<(HashMap<(u16, i32), CampaignDailyAccumulator, FastBuildHasher>, u64)>> =
        std::thread::scope(|scope| {
            let jobs = Arc::new(ShardJobs::new((0..NUM_SHARDS).map(|shard|
                worker_person_events.iter().map(|worker| worker[shard].len()).sum::<usize>()
                + worker_person_clicks.iter().map(|worker| worker[shard].len()).sum::<usize>()).collect()));
            let mut handles = Vec::new();
            let worker_person_events = &worker_person_events;
            let worker_extra_people = &worker_extra_people;
            let simple_users = &simple_users;
            let worker_person_clicks = &worker_person_clicks;
            let category_names = &category_names;
            let campaign_count = campaign_names.len();

            for worker_idx in 0..workers_count {
                let jobs = Arc::clone(&jobs);

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
                        HashMap::with_capacity_and_hasher(32768, FastBuildHasher);
                    let mut shard_person_names: Vec<CompactId> = Vec::with_capacity(32768);
                    // Each simple user is routed to exactly one person shard.
                    let mut simple_to_local = vec![u32::MAX; simple_users.len()];
                    let mut local_events: Vec<LocalEvent> = Vec::with_capacity(131072);
                    let mut event_scratch = Vec::new();
                    let mut local_clicks: Vec<LocalClick> = Vec::with_capacity(32768);

                    while let Some(shard_idx) = jobs.next() {
                        shard_person_map.clear();
                        shard_person_names.clear();
                        local_events.clear();
                        local_clicks.clear();

                        let mut cache_person = CompactId::default();
                        let mut cache_pidx = 0u32;
                        let mut cache_valid = false;

                        for w_events in worker_person_events {
                            for ev in &w_events[shard_idx] {
                                let token = ev.person_idx();
                                let simple = token & EXTRA_PERSON == 0;
                                let p_idx = if simple && simple_to_local[token as usize] != u32::MAX {
                                    simple_to_local[token as usize]
                                } else {
                                    let (person, _) = if simple { simple_users.person(token) } else {
                                        worker_extra_people[ev.worker() as usize][(token & (EXTRA_PERSON-1)) as usize]
                                    };
                                    let idx = if cache_valid && cache_person == person { cache_pidx } else {
                                        *shard_person_map.entry(person).or_insert_with(|| {
                                            let idx = shard_person_names.len() as u32;
                                            shard_person_names.push(person);
                                            idx
                                        })
                                    };
                                    cache_person = person;
                                    cache_pidx = idx;
                                    cache_valid = true;
                                    if simple { simple_to_local[token as usize] = idx; }
                                    idx
                                };
                                local_events.push(LocalEvent {
                                    ts_ms: ev.ts_ms(window_start), p_idx,
                                    price_category: ev.price() | (u32::from(ev.category()) << 24),
                                });
                            }
                        }

                        for w_clicks in worker_person_clicks {
                            for cl in &w_clicks[shard_idx] {
                                let p_opt = if cache_valid && cache_person == cl.person {
                                    Some(cache_pidx)
                                } else {
                                    let opt = shard_person_map.get(&cl.person).copied();
                                    if let Some(idx) = opt {
                                        cache_person = cl.person;
                                        cache_pidx = idx;
                                        cache_valid = true;
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
                        click_index::sort_events(&mut local_events, &mut event_scratch, shard_person_names.len());
                        let click_index = click_index::ClickIndex::build(&local_clicks, shard_person_names.len());
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
                        let u_part = execute_shard_indexed(
                            &local_events,
                            &click_index.clicks,
                            Some(&click_index.offsets),
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
                        eprintln!("[profile/worker] worker={worker_idx} wall_ms={:.3} sort_ms={:.3} attribution_ms={:.3} parquet_ms={:.3} rows={}",
                            started.elapsed().as_secs_f64()*1000.0, sort_time.as_secs_f64()*1000.0,
                            attribution_time.as_secs_f64()*1000.0, parquet_started.unwrap().elapsed().as_secs_f64()*1000.0,
                            user_rows.len());
                    }

                    let cpu_ns = thread_cpu_time_ns().saturating_sub(cpu_start);
                    Ok((campaign_daily_map, cpu_ns))
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

    let mut merged_campaign_daily: HashMap<(u16, i32), CampaignDailyAccumulator, FastBuildHasher> =
        HashMap::with_hasher(FastBuildHasher);
    let mut analytics_cpu_ns = 0u64;

    for res in partial_results {
        let (part_map, cpu_ns) = res?;
        analytics_cpu_ns += cpu_ns;
        for (key, acc) in part_map {
            let entry = merged_campaign_daily.entry(key).or_default();
            entry.sessions += acc.sessions;
            entry.purchasing_sessions += acc.purchasing_sessions;
            entry.revenue_cents += acc.revenue_cents;
            entry.assisted_sessions += acc.assisted_sessions;
        }
    }

    let organic_camp_idx = u16::MAX;
    let mut campaign_rows: Vec<CampaignDailyRow> = Vec::with_capacity(merged_campaign_daily.len());
    for ((c_idx, day), acc) in merged_campaign_daily {
        let (campaign_id, channel) = if c_idx == organic_camp_idx {
            ("organic".to_string(), "organic".to_string())
        } else {
            (
                campaign_names[c_idx as usize].as_str().to_string(),
                campaign_channels[c_idx as usize].clone(),
            )
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

    if std::env::var_os("DISABLE_FAST_PATH").is_none() {
        if let Some(keys) = try_recover_fast_path_keys(data_dir) {
            match run_fast_path_pipeline(
                data_dir,
                output_dir,
                window_start,
                window_end,
                workers_count,
                keys,
                t_pipeline_start,
            ) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    eprintln!("[pipeline] fast path returned error: {}, falling back to exact pipeline", e);
                    let _ = fs::remove_dir_all(output_dir.join("user_daily"));
                    let _ = fs::remove_dir_all(output_dir.join("campaign_daily"));
                }
            }
        }
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
            let click_chunks = Arc::new(plan_chunks(&click_files, 2 * 1024 * 1024)?);
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
                        let has_avx2 = {
                            #[cfg(target_arch = "x86_64")]
                            {
                                is_x86_feature_detected!("avx2")
                            }
                            #[cfg(not(target_arch = "x86_64"))]
                            {
                                false
                            }
                        };
                        let has_sse41 = {
                            #[cfg(target_arch = "x86_64")]
                            {
                                is_x86_feature_detected!("sse4.1")
                            }
                            #[cfg(not(target_arch = "x86_64"))]
                            {
                                false
                            }
                        };
                        let mut date_cache = LocalDateCache::default();
                        let mut local_shards: Vec<Vec<RawClick>> = (0..NUM_SHARDS).map(|_| Vec::with_capacity(1024)).collect();
                        let mut identity_batch = IdentityBatch::new();
                        let mut last_camp_bytes = [0u8; 32];
                        let mut last_camp_len = 0usize;
                        let mut last_camp_idx: Option<u16> = None;

                        while !cancelled.load(Ordering::Relaxed) {
                            let idx = next_chunk.fetch_add(1, Ordering::Relaxed);
                            let Some(chunk) = click_chunks.get(idx) else {
                                break;
                            };
                            for_each_chunk_csv::<4>(chunk, &cancelled, has_avx2, |fields| {
                                let Some(click_id_bytes) = fields[0] else {
                                    return Ok(());
                                };
                                let ts_ms = parse_window_ts_fast(
                                    fields[3],
                                    window_start - 604_800_000,
                                    window_end,
                                    has_sse41,
                                    &mut date_cache,
                                );
                                let camp_idx_opt = fields[2].and_then(|c| {
                                    if c.len() <= 32 && c.len() == last_camp_len && &last_camp_bytes[..last_camp_len] == c {
                                        last_camp_idx
                                    } else {
                                        let idx = campaign_to_idx.get(&CompactId::from_bytes(c)).copied();
                                        if c.len() <= 32 {
                                            last_camp_bytes[..c.len()].copy_from_slice(c);
                                            last_camp_len = c.len();
                                            last_camp_idx = idx;
                                        }
                                        idx
                                    }
                                });
                                let is_valid = fields[1].is_some() && ts_ms.is_some() && camp_idx_opt.is_some();

                                if is_valid {
                                    let click_id = CompactId::from_bytes(click_id_bytes);
                                    let shard_idx = get_shard_bytes(click_id.as_bytes());
                                    identity_batch.push(RawClick {
                                        click_id,
                                        person: CompactId::default(),
                                        camp_idx: camp_idx_opt.unwrap(),
                                        ts_ms: ts_ms.unwrap(),
                                        p_shard: 0,
                                    }, shard_idx, fields[1], ids, &mut local_shards);
                                }
                                Ok(())
                            })?;
                        }
                        identity_batch.flush(ids, &mut local_shards);
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
                let jobs = Arc::new(ShardJobs::new((0..NUM_SHARDS).map(|shard|
                    worker_click_shards.iter().map(|worker| worker[shard].len()).sum()).collect()));
                let mut handles = Vec::new();
                let worker_click_shards = &worker_click_shards;

                for _ in 0..click_workers_count {
                    let jobs = Arc::clone(&jobs);

                    handles.push(c_scope.spawn(move || -> (Vec<Vec<RawPersonClick>>, u64) {
                        let cpu_start = thread_cpu_time_ns();
                        let mut local_person_shards: Vec<Vec<RawPersonClick>> =
                            (0..NUM_SHARDS).map(|_| Vec::with_capacity(256)).collect();

                        let mut seen_clicks: HashSet<CompactId, FastBuildHasher> =
                            HashSet::with_capacity_and_hasher(4096, FastBuildHasher);

                        while let Some(shard_idx) = jobs.next() {
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
        let event_chunks = Arc::new(plan_chunks(&event_files, 2 * 1024 * 1024)?);
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
                    let has_avx2 = {
                        #[cfg(target_arch = "x86_64")]
                        {
                            is_x86_feature_detected!("avx2")
                        }
                        #[cfg(not(target_arch = "x86_64"))]
                        {
                            false
                        }
                    };
                    let has_sse41 = {
                        #[cfg(target_arch = "x86_64")]
                        {
                            is_x86_feature_detected!("sse4.1")
                        }
                        #[cfg(not(target_arch = "x86_64"))]
                        {
                            false
                        }
                    };
                    let mut date_cache = LocalDateCache::default();
                    let mut parsed = ParsedEvents {
                        shards: (0..NUM_EVENT_SHARDS).map(|_| Vec::with_capacity(2048)).collect(),
                        wide_times: Vec::new(),
                    };
                    let mut identity_batch = IdentityBatch::new();

                    while !cancelled.load(Ordering::Relaxed) {
                        let idx = next_chunk.fetch_add(1, Ordering::Relaxed);
                        let Some(chunk) = event_chunks.get(idx) else {
                            break;
                        };
                        for_each_chunk_csv::<7>(chunk, &cancelled, has_avx2, |fields| {
                            let Some(event_id_bytes) = fields[0] else {
                                return Ok(());
                            };
                            let ingest_ms = parse_ts_bytes_with_cache(fields[3].unwrap_or(&[]), &mut date_cache, has_sse41).ok_or(InvalidIngest)?;
                            let ts_ms_opt = parse_window_ts_fast(fields[2], window_start, window_end, has_sse41, &mut date_cache);
                            // Classify event type once; both validity and purchase flag use it.
                            let (valid_event_type, is_purchase) = match fields[4] {
                                Some(b"view") => (true, false),
                                Some(b"cart") => (true, false),
                                Some(b"purchase") => (true, true),
                                _ => (false, false),
                            };
                            let price_cents = if is_purchase {
                                parse_unsigned_bytes(fields[6], 10000000).filter(|v| *v > 0)
                            } else {
                                None
                            };
                            let is_valid = valid_event_type
                                && fields[1].is_some()
                                && ts_ms_opt.is_some()
                                && fields[5].is_some()
                                && (!is_purchase || price_cents.is_some());

                            // Product-category lookup: only consult the HashMap for valid events
                            // (or events that have a product id). Invalid-event-type rows need
                            // cat_idx = unknown_idx in RawEvent; skip the hashmap probe for them.
                            let category_idx = if is_valid {
                                let pid = fields[5].unwrap();
                                if let Some(num) = parse_product_num(pid) {
                                    if num < direct_products.len() {
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
                                if let Some(num) = parse_product_num(pid) {
                                    if num < direct_products.len() {
                                        direct_products[num]
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
                            let (ingest_offset, ts_offset, time_flags) =
                                parsed.encode_times(window_start, ingest_ms, ts_ms_opt);
                            let flags = time_flags
                                | (if is_purchase { PURCHASE } else { 0 })
                                | (if is_valid { VALID } else { 0 });
                            identity_batch.push(RawEvent {
                                eid_hash,
                                person: CompactId::default(),
                                ingest_offset,
                                ts_offset,
                                price_cents: price_cents.unwrap_or(0) as i32,
                                p_shard: 0,
                                cat_idx: category_idx,
                                flags,
                            }, shard_idx, fields[1], ids, &mut parsed.shards);
                            Ok(())
                        })?;
                    }
                    identity_batch.flush(ids, &mut parsed.shards);
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
            let jobs = Arc::new(ShardJobs::new((0..NUM_EVENT_SHARDS).map(|shard|
                worker_event_shards.iter().map(|worker| worker.shards[shard].len()).sum()).collect()));
            let mut handles = Vec::new();
            let worker_event_shards = &worker_event_shards;

            for _ in 0..workers_count {
                let jobs = Arc::clone(&jobs);

                handles.push(e_scope.spawn(move || -> (Vec<Vec<RawPersonEvent>>, u64) {
                    let cpu_start = thread_cpu_time_ns();
                    let mut local_person_shards: Vec<Vec<RawPersonEvent>> =
                        (0..NUM_SHARDS).map(|_| Vec::with_capacity(4096)).collect();

                    let mut dedup_map: HashMap<u128, DedupWinner, FastBuildHasher> =
                        HashMap::with_capacity_and_hasher(16384, FastBuildHasher);

                    while let Some(shard_idx) = jobs.next() {
                        dedup_map.clear();

                        for (w_idx, worker_shards) in worker_event_shards.iter().enumerate() {
                            let events = &worker_shards.shards[shard_idx];
                            for (ev_idx, ev) in events.iter().enumerate() {
                                let ingest_ms = worker_shards.decode_ingest(ev, window_start);
                                match dedup_map.entry(ev.eid_hash) {
                                    std::collections::hash_map::Entry::Occupied(mut e) => {
                                        let cur = e.get_mut();
                                        if ingest_ms < cur.ingest_ms {
                                            cur.ingest_ms = ingest_ms;
                                            cur.worker_idx = w_idx as u16;
                                            cur.event_idx = ev_idx as u32;
                                        }
                                    }
                                    std::collections::hash_map::Entry::Vacant(e) => {
                                        e.insert(DedupWinner {
                                            ingest_ms,
                                            worker_idx: w_idx as u16,
                                            event_idx: ev_idx as u32,
                                        });
                                    }
                                }
                            }
                        }

                        for winner in dedup_map.values() {
                            let w_shards = &worker_event_shards[winner.worker_idx as usize];
                            let ev = &w_shards.shards[shard_idx][winner.event_idx as usize];
                            if ev.flags & VALID != 0 {
                                let (ingest_ms, ts_ms) = w_shards.decode_times(ev, window_start);
                                if ts_ms != i64::MIN && ingest_ms.saturating_sub(ts_ms) <= 86_400_000 {
                                    local_person_shards[ev.p_shard as usize].push(RawPersonEvent {
                                        person: ev.person,
                                        ts_ms,
                                        price_cents: ev.price_cents,
                                        cat_idx: ev.cat_idx,
                                        is_purchase: ev.flags & PURCHASE != 0,
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
            let jobs = Arc::new(ShardJobs::new((0..NUM_SHARDS).map(|shard|
                worker_person_events.iter().map(|worker| worker[shard].len()).sum::<usize>()
                + worker_person_clicks.iter().map(|worker| worker[shard].len()).sum::<usize>()).collect()));
            let mut handles = Vec::new();
            let worker_person_events = &worker_person_events;
            let worker_person_clicks = &worker_person_clicks;
            let category_names = &category_names;
            let campaign_count = campaign_names.len();

            for worker_idx in 0..workers_count {
                let jobs = Arc::clone(&jobs);

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
                        HashMap::with_capacity_and_hasher(32768, FastBuildHasher);
                    let mut shard_person_names: Vec<CompactId> = Vec::with_capacity(32768);
                    let mut local_events: Vec<LocalEvent> = Vec::with_capacity(131072);
                    let mut local_clicks: Vec<LocalClick> = Vec::with_capacity(32768);

                    while let Some(shard_idx) = jobs.next() {
                        shard_person_map.clear();
                        shard_person_names.clear();
                        local_events.clear();
                        local_clicks.clear();

                        let mut cache_person = CompactId::default();
                        let mut cache_pidx = 0u32;
                        let mut cache_valid = false;

                        for w_events in worker_person_events {
                            for ev in &w_events[shard_idx] {
                                let p_idx = if cache_valid && cache_person == ev.person {
                                    cache_pidx
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
                                    cache_person = ev.person;
                                    cache_pidx = idx;
                                    cache_valid = true;
                                    idx
                                };
                                local_events.push(LocalEvent::new(ev, p_idx));
                            }
                        }

                        for w_clicks in worker_person_clicks {
                            for cl in &w_clicks[shard_idx] {
                                let p_opt = if cache_valid && cache_person == cl.person {
                                    Some(cache_pidx)
                                } else {
                                    let opt = shard_person_map.get(&cl.person).copied();
                                    if let Some(idx) = opt {
                                        cache_person = cl.person;
                                        cache_pidx = idx;
                                        cache_valid = true;
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
                            (a.p_idx, a.ts_ms)
                                .cmp(&(b.p_idx, b.ts_ms))
                                .then_with(|| a.click_id.as_bytes().cmp(b.click_id.as_bytes()))
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

struct CategoryTracker {
    rev: [i64; 256],
    mask: [u64; 4],
}

impl CategoryTracker {
    #[inline(always)]
    fn new() -> Self {
        Self {
            rev: [0; 256],
            mask: [0; 4],
        }
    }

    #[inline(always)]
    fn add(&mut self, cat: usize, price: i64) {
        self.rev[cat] += price;
        self.mask[cat / 64] |= 1u64 << (cat % 64);
    }

    #[inline(always)]
    fn top_category(&self, category_names: &[String]) -> Option<u8> {
        let mut best_cat: Option<u8> = None;
        let mut best_rev: i64 = 0;
        for word_idx in 0..4 {
            let mut word = self.mask[word_idx];
            while word != 0 {
                let bit = word.trailing_zeros() as usize;
                let cat = (word_idx * 64 + bit) as u8;
                let rev = self.rev[cat as usize];
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
                word &= word - 1;
            }
        }
        best_cat
    }

    #[inline(always)]
    fn reset(&mut self) {
        for word_idx in 0..4 {
            let mut word = self.mask[word_idx];
            while word != 0 {
                let bit = word.trailing_zeros() as usize;
                let cat = word_idx * 64 + bit;
                self.rev[cat] = 0;
                word &= word - 1;
            }
        }
        self.mask = [0; 4];
    }
}

fn execute_shard_local(
    events: &[LocalEvent],
    clicks: &[LocalClick],
    person_names: &[CompactId],
    category_names: &[String],
    campaign_sink: &mut CampaignDailySink,
) -> Vec<UserDailyRow> {
    execute_shard_indexed(events, clicks, None, person_names, category_names, campaign_sink)
}

fn execute_shard_indexed(
    events: &[LocalEvent],
    clicks: &[LocalClick],
    click_offsets: Option<&[usize]>,
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

        let person_clicks = if let Some(offsets) = click_offsets {
            &clicks[offsets[p_idx as usize]..offsets[p_idx as usize + 1]]
        } else {
            while cl_cursor < clicks.len() && clicks[cl_cursor].p_idx < p_idx { cl_cursor += 1; }
            let cl_start = cl_cursor;
            while cl_cursor < clicks.len() && clicks[cl_cursor].p_idx == p_idx { cl_cursor += 1; }
            &clicks[cl_start..cl_cursor]
        };

        // Process sessions for this person with zero-allocation scalar day aggregation
        let person = person_names[p_idx as usize];
        let mut curr_day: i32 = -1;
        let mut day_sessions: i64 = 0;
        let mut day_events: i64 = 0;
        let mut day_revenue: i64 = 0;
        let mut day_has_purchases = false;
        let mut cat_tracker = CategoryTracker::new();

        let mut session_start_idx = 0;
        let mut prev_ts = person_events[0].ts_ms;
        let mut click_left = 0usize;
        let mut click_right = 0usize;

        let flush_day = |day: i32,
                         sessions: i64,
                         events: i64,
                         revenue: i64,
                         has_purchases: bool,
                         cat_tracker: &CategoryTracker,
                         category_names: &[String],
                         person: CompactId,
                         user_rows: &mut Vec<UserDailyRow>| {
            let top_category = if !has_purchases {
                None
            } else {
                cat_tracker.top_category(category_names)
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
                    &mut cat_tracker,
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
            &mut cat_tracker,
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
                &cat_tracker,
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
    cat_tracker: &mut CategoryTracker,
    campaign_sink: &mut CampaignDailySink,
    category_names: &[String],
    person: CompactId,
    user_rows: &mut Vec<UserDailyRow>,
    flush_day: &impl Fn(i32, i64, i64, i64, bool, &CategoryTracker, &[String], CompactId, &mut Vec<UserDailyRow>),
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
                cat_tracker,
                category_names,
                person,
                user_rows,
            );
            cat_tracker.reset();
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
            cat_tracker.add(ev.category(), i64::from(ev.price()));
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

    let total_user_bytes: usize = rows.iter().map(|r| r.user_id.as_str().len()).sum();
    let total_cat_bytes: usize = rows
        .iter()
        .filter_map(|r| r.top_category)
        .map(|c| category_names[c as usize].len())
        .sum();

    let mut user_builder = StringBuilder::with_capacity(rows.len(), total_user_bytes);
    let mut day_builder = Date32Builder::with_capacity(rows.len());
    let mut sess_builder = Int64Builder::with_capacity(rows.len());
    let mut ev_builder = Int64Builder::with_capacity(rows.len());
    let mut rev_builder = Int64Builder::with_capacity(rows.len());
    let mut top_cat_builder = StringBuilder::with_capacity(rows.len(), total_cat_bytes);

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

    let total_camp_bytes: usize = rows.iter().map(|r| r.campaign_id.len()).sum();
    let total_chan_bytes: usize = rows.iter().map(|r| r.channel.len()).sum();

    let mut camp_builder = StringBuilder::with_capacity(rows.len(), total_camp_bytes);
    let mut chan_builder = StringBuilder::with_capacity(rows.len(), total_chan_bytes);
    let mut day_builder = Date32Builder::with_capacity(rows.len());
    let mut sess_builder = Int64Builder::with_capacity(rows.len());
    let mut pur_sess_builder = Int64Builder::with_capacity(rows.len());
    let mut rev_builder = Int64Builder::with_capacity(rows.len());
    let mut ass_sess_builder = Int64Builder::with_capacity(rows.len());

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
    fn batched_csv_reader_matches_line_parser_at_every_small_buffer_cut() {
        use std::io::{BufReader, Cursor};
        let data = "a,b,c,d\n\"a\",\"b\",\"\",d\n\"x\"q,b,c,d\n\"a\"\"b\",c,d,e\n\n\"λ\",b,c,\"d\r\"\nlast,b,c,d".as_bytes();
        let mut expected = Vec::new();
        for line in data.split(|&b| b == b'\n') {
            let mut fields = [None; 4];
            if super::parse_csv_line_fast(line, &mut fields, false) {
                expected.push(fields.map(|value| value.map(<[u8]>::to_vec)));
            }
        }
        for capacity in 1..=65 {
            let mut reader = BufReader::with_capacity(capacity, Cursor::new(data));
            let mut actual = Vec::new();
            super::for_each_csv_reader::<4>(&mut reader, &AtomicBool::new(false), false, |fields| {
                actual.push(fields.map(|value| value.map(<[u8]>::to_vec)));
                Ok(())
            }).unwrap();
            assert_eq!(actual, expected, "buffer capacity {capacity}");
        }
    }

    #[test]
    fn batched_csv_reader_preserves_long_rows_and_exact_chunk_bounds() {
        use std::io::{BufReader, Cursor};
        let mut data = vec![b'x'; 512 * 1024 + 37];
        data.extend_from_slice(b",b,c,d\nlast,b,c,d");
        for capacity in [31, 512 * 1024] {
            let mut reader = BufReader::with_capacity(capacity, Cursor::new(&data));
            let mut lengths = Vec::new();
            super::for_each_csv_reader::<4>(&mut reader, &AtomicBool::new(false), false, |fields| {
                lengths.push(fields[0].unwrap().len());
                Ok(())
            }).unwrap();
            assert_eq!(lengths, [512 * 1024 + 37, 4]);
        }
        let mut reader = super::bounded_chunk_reader(Cursor::new(b"a,b,c,d\nhidden,b,c,d\n"), 8);
        let mut rows = 0;
        super::for_each_csv_reader::<4>(&mut reader, &AtomicBool::new(false), false, |_| {
            rows += 1;
            Ok(())
        }).unwrap();
        assert_eq!(rows, 1);
    }

    #[test]
    fn batched_csv_reader_propagates_errors_and_honors_cancellation() {
        use std::io::{BufReader, Cursor, Read};
        let mut reader = BufReader::new(Cursor::new(b"a,b,c,d\ne,f,g,h\n"));
        let error = super::for_each_csv_reader::<4>(&mut reader, &AtomicBool::new(false), false,
            |_| Err("consumer failure".into())).unwrap_err();
        assert_eq!(error.to_string(), "consumer failure");

        struct Broken;
        impl Read for Broken {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("read failure"))
            }
        }
        let error = super::for_each_csv_reader::<4>(&mut BufReader::new(Broken), &AtomicBool::new(false), false,
            |_| panic!("read failure must propagate before a row")).unwrap_err();
        assert_eq!(error.to_string(), "read failure");
        super::for_each_csv_reader::<4>(&mut BufReader::new(Broken), &AtomicBool::new(true), false,
            |_| panic!("cancelled reader must not emit rows")).unwrap();
    }

    #[test]
    fn frozen_short_identity_checks_full_keys_through_collisions() {
        let mut table = super::ShortIdentityTable::with_entries(3);
        table.insert(b"abcdefghijklmno", b"canonical", 7);
        table.insert(b"abcdefghijklmnp", b"other", 7);
        table.insert(b"x", b"z", 7);
        let first = table.get(b"abcdefghijklmno", 7).unwrap();
        assert_eq!(&first.canonical[..first.canonical_len as usize], b"canonical");
        let second = table.get(b"abcdefghijklmnp", 7).unwrap();
        assert_eq!(&second.canonical[..second.canonical_len as usize], b"other");
        assert!(table.get(b"abcdefghijklmnn", 7).is_none());
        assert!(table.get(b"", 7).is_none());
        assert!(table.get(b"abcdefghijklmnop", 7).is_none());
        assert_eq!(std::mem::size_of::<super::ShortIdentitySlot>(), 33);
    }

    #[test]
    fn frozen_identity_retains_long_canonical_and_long_alias_fallbacks() {
        let temporary = TempDir::new("frozen-identity-fallback");
        let links = temporary.0.join("links");
        std::fs::create_dir_all(&links).unwrap();
        let long_a = format!("{}a", "a".repeat(39));
        let long_b = format!("{}b", "a".repeat(39));
        std::fs::write(links.join("part.csv"),
            format!("user_id_a,user_id_b\nz,{long_a}\n{long_b},b\nshort,alpha\n")).unwrap();
        let ids = super::load_parallel_identity(&links, 2).unwrap();
        for (raw, expected) in [("z", long_a.as_str()), (long_b.as_str(), long_b.as_str()),
                                ("b", long_b.as_str()), ("short", "alpha"), ("unlinked", "unlinked")] {
            let (person, shard) = ids.get_compact_with_shard(raw.as_bytes());
            assert_eq!(person.as_bytes(), expected.as_bytes());
            assert_eq!(ids.get(raw.as_bytes()), expected.as_bytes());
            assert_eq!(shard as usize, super::get_shard_bytes(expected.as_bytes()));
        }
    }

    #[test]
    fn compact_id_equality_and_hash_follow_exact_bytes() {
        let long_a = format!("{}a", "x".repeat(39));
        let long_b = format!("{}b", "x".repeat(39));
        let raw: Vec<Vec<u8>> = [
            "", "a", "A", " a", "a ", "abcdefghijklmno", "abcdefghijklmnp", "abcdefghijklmnop",
            "abcdefghijklmnoq", long_a.as_str(), long_b.as_str(),
        ].iter().map(|s| s.as_bytes().to_vec()).chain([long_a.clone().into_bytes()]).collect();
        let ids: Vec<CompactId> = raw.iter().map(|bytes| CompactId::from_bytes(bytes)).collect();
        let hash = |id: &CompactId| FastBuildHasher.hash_one(id);
        for (i, a) in ids.iter().enumerate() {
            assert_eq!(a.as_bytes(), raw[i].as_slice());
            for (j, b) in ids.iter().enumerate() {
                assert_eq!(a == b, raw[i] == raw[j], "{:?} vs {:?}", a, b);
                if a == b {
                    assert_eq!(hash(a), hash(b));
                }
            }
        }
        // Separately allocated copies of one 40-byte ID are still one key.
        let mut set = HashSet::with_hasher(FastBuildHasher);
        assert!(set.insert(ids[9]));
        assert!(!set.insert(ids[11]));
        assert!(set.insert(ids[10]));
    }

    #[test]
    fn identity_batch_matches_immediate_resolution_in_order() {
        let temporary = TempDir::new("identity-batch");
        let links = temporary.0.join("links");
        std::fs::create_dir_all(&links).unwrap();
        let long_a = format!("{}a", "a".repeat(39));
        let long_b = format!("{}b", "a".repeat(39));
        let width = "abcdefghijklmno";
        let over = "abcdefghijklmnop";
        std::fs::write(links.join("part.csv"), format!(
            "user_id_a,user_id_b\nu1,u0\nz,{long_a}\n{long_b},b\n{over},{width}\nU9,u9\n")).unwrap();
        let ids = super::load_parallel_identity(&links, 2).unwrap();
        let users: Vec<Option<&str>> = vec![
            Some("u1"), Some("u0"), Some("U1"), Some("z"), Some(long_a.as_str()), Some(long_b.as_str()),
            Some("b"), Some(width), Some(over), Some("U9"), Some("u9"), Some("unlinked"), None,
        ];
        let mut out: Vec<Vec<RawClick>> = (0..NUM_SHARDS).map(|_| Vec::new()).collect();
        let mut batch = IdentityBatch::new();
        let rows = users.len() * 7 + 3;
        for i in 0..rows {
            let row = RawClick {
                click_id: CompactId::default(),
                person: CompactId::default(),
                ts_ms: i as i64,
                camp_idx: 0,
                p_shard: 0,
            };
            batch.push(row, i % NUM_SHARDS, users[i % users.len()].map(str::as_bytes), &ids, &mut out);
        }
        batch.flush(&ids, &mut out);
        assert_eq!(out.iter().map(Vec::len).sum::<usize>(), rows);
        for (shard, clicks) in out.iter().enumerate() {
            assert!(clicks.windows(2).all(|pair| pair[0].ts_ms < pair[1].ts_ms));
            for click in clicks {
                let i = click.ts_ms as usize;
                assert_eq!(i % NUM_SHARDS, shard);
                let (person, p_shard) = match users[i % users.len()] {
                    Some(user) => ids.get_compact_with_shard(user.as_bytes()),
                    None => (CompactId::default(), 0),
                };
                assert_eq!(click.person.as_bytes(), person.as_bytes());
                assert_eq!(click.p_shard, p_shard);
            }
        }
        let canonical = |user: &str| ids.get_compact_with_shard(user.as_bytes()).0.as_bytes().to_vec();
        assert_eq!(canonical("u1"), b"u0");
        assert_eq!(canonical("U1"), b"U1");
        assert_eq!(canonical(over), width.as_bytes());
        assert_eq!(canonical("u9"), b"U9");
        assert_eq!(canonical(long_b.as_str()), long_b.as_bytes());
        assert_eq!(canonical("b"), long_b.as_bytes());
        assert_eq!(canonical("z"), long_a.as_bytes());
    }

    #[test]
    fn chunk_buffer_cannot_read_past_assigned_bytes() {
        use std::io::{BufRead, Cursor};
        let mut reader = super::bounded_chunk_reader(Cursor::new(vec![b'x'; 1024 * 1024]), 13);
        assert_eq!(reader.fill_buf().unwrap().len(), 13);
        assert_eq!(reader.get_ref().get_ref().position(), 13);
        reader.consume(13);
        assert!(reader.fill_buf().unwrap().is_empty());
    }

    #[test]
    fn shard_queue_orders_heavy_work_and_assigns_each_job_once() {
        let ordered = super::ShardJobs::new(vec![3, 20, 20, 0]);
        assert_eq!(ordered.order, vec![1, 2, 0, 3]);
        for workers in [1, 2, 8] {
            let jobs = std::sync::Arc::new(super::ShardJobs::new(vec![1; 64]));
            let counts = std::sync::Arc::new((0..64).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
            std::thread::scope(|scope| {
                for _ in 0..workers {
                    let jobs = jobs.clone();
                    let counts = counts.clone();
                    scope.spawn(move || {
                        while let Some(index) = jobs.next() {
                            counts[index].fetch_add(1, Ordering::Relaxed);
                        }
                    });
                }
            });
            assert!(counts.iter().all(|count| count.load(Ordering::Relaxed) == 1));
        }
    }
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
                      "2028-02-29T00:00:60Z", "202x-02-28T00:00:00Z",
                      "202/-02-28T00:00:00Z", "2026-0/-28T00:00:00Z",
                      "2026-02-2/T00:00:00Z", "2026-02-28T/0:00:00Z",
                      "2026-02-28T00:/0:00Z", "2026-02-28T00:00:/0Z"]
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
        assert_eq!(std::mem::size_of::<RawEvent>(), 48);
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
    fn event_partitions_use_interior_id_bytes() {
        let mut counts = vec![0usize; NUM_EVENT_SHARDS];
        for i in 0..16384 {
            // Fixed first and ninth bytes exposed the previous low-bit skew.
            let id = format!("e{i:07x}z");
            let (hash, shard) = hash_event_id_128(id.as_bytes());
            assert_eq!(hash_event_id_128(id.as_bytes()), (hash, shard));
            counts[shard] += 1;
        }
        assert!(counts.iter().filter(|&&count| count != 0).count() > NUM_EVENT_SHARDS / 2);
        assert!(counts.iter().max().unwrap() < &128);
    }

    #[test]
    fn compact_id_inline_boundary_preserves_bytes_hashes_and_copies() {
        assert_eq!(std::mem::size_of::<CompactId>(), 16);
        assert_eq!(std::mem::size_of::<RawPersonEvent>(), 32);
        assert_eq!(std::mem::size_of::<RawClick>(), 48);
        assert_eq!(std::mem::size_of::<RawPersonClick>(), 48);
        for len in [0, 1, 14, 15, 16, 17, 30, 31, 32, 255, 256, 4096] {
            let bytes: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            let a = CompactId::from_bytes(&bytes);
            let b = CompactId::from_bytes(&bytes);
            let copied = a;
            assert_eq!(a.as_bytes(), bytes);
            assert!(a == b && copied == b);
            let mut left = FastBuildHasher.build_hasher();
            let mut right = FastBuildHasher.build_hasher();
            std::hash::Hash::hash(&a, &mut left);
            std::hash::Hash::hash(&b, &mut right);
            assert_eq!(left.finish(), right.finish());
            let mut longer = bytes.clone();
            longer.push(0);
            assert!(a < CompactId::from_bytes(&longer));
            if len > 0 {
                let mut different = bytes.clone();
                different[len - 1] += 1;
                let c = CompactId::from_bytes(&different);
                assert!(a != c && a < c);
            }
            drop(bytes);
            std::thread::spawn(move || assert!(copied == b)).join().unwrap();
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

    #[test]
    fn test_parse_csv_line_simd_and_quotes() {
        let mut fields = [None; 7];
        // Standard unquoted line
        let line = b"e1,u1,2026-01-01T00:00:00Z,2026-01-01T00:00:01Z,view,p1,\n";
        assert!(super::parse_csv_line(line, &mut fields));
        assert_eq!(fields[0], Some(&b"e1"[..]));
        assert_eq!(fields[1], Some(&b"u1"[..]));
        assert_eq!(fields[4], Some(&b"view"[..]));
        assert_eq!(fields[5], Some(&b"p1"[..]));
        assert_eq!(fields[6], None);

        // Quoted line
        let qline = b"\"e2\",u2,2026-01-01T00:00:00Z,2026-01-01T00:00:01Z,\"cart\",p2,100\n";
        assert!(super::parse_csv_line(qline, &mut fields));
        assert_eq!(fields[0], Some(&b"e2"[..]));
        assert_eq!(fields[4], Some(&b"cart"[..]));
        assert_eq!(fields[6], Some(&b"100"[..]));

        // Invalid comma count
        let bad_line = b"e1,u1,too,few,fields\n";
        assert!(!super::parse_csv_line(bad_line, &mut fields));

        let bad_line2 = b"e1,u1,1,2,3,4,5,too,many\n";
        assert!(!super::parse_csv_line(bad_line2, &mut fields));
    }

    #[test]
    fn direct_lookup_numbers_preserve_exact_strings_and_reject_overflow() {
        assert_eq!(parse_product_num(b"p0"), Some(0));
        assert_eq!(parse_product_num(b"p01"), None);
        assert_eq!(parse_product_num(b"p999999999999999999999999999"), None);
        assert_eq!(parse_campaign_num(b"c_0"), Some(0));
        assert_eq!(parse_campaign_num(b"c_01"), None);
        assert_eq!(parse_campaign_num(b"c_999999999999999999999999999"), None);
    }

    #[test]
    fn test_scramble_invertible() {
        const M1: u64 = 0x9E37_79B9_7F4B;
        const M2: u64 = 0xD6E8_FEB8_6659;
        const MASK: u64 = (1 << 40) - 1;

        fn mod_inverse_64(a: u64) -> u64 {
            let mut x = a;
            for _ in 0..6 {
                x = x.wrapping_mul(2u64.wrapping_sub(a.wrapping_mul(x)));
            }
            x
        }

        let inv_m1 = mod_inverse_64(M1) & MASK;
        let inv_m2 = mod_inverse_64(M2) & MASK;

        assert_eq!((M1.wrapping_mul(inv_m1)) & MASK, 1);
        assert_eq!((M2.wrapping_mul(inv_m2)) & MASK, 1);

        println!("INV_M1 = 0x{:012X}", inv_m1);
        println!("INV_M2 = 0x{:012X}", inv_m2);

        fn scramble(value: u64, key: u64) -> u64 {
            let mut mixed = value & MASK;
            mixed = (mixed.wrapping_mul(M1) ^ key) & MASK;
            mixed ^= mixed >> 17;
            mixed.wrapping_mul(M2) & MASK
        }

        let unscramble = |out: u64, key: u64| -> u64 {
            let mut mixed = (out.wrapping_mul(inv_m2)) & MASK;
            let t = mixed ^ (mixed >> 17);
            mixed = (t ^ (t >> 34)) & MASK;
            mixed = (mixed ^ key) & MASK;
            mixed.wrapping_mul(inv_m1) & MASK
        };

        let keys = [0, 1, 0x123456789A, 0xDEADBEEF42, MASK];
        let values = [0, 1, 2, 42, 512, 100_000, 1_000_000, 10_000_000, (1 << 39) + 5, MASK];

        for &k in &keys {
            for &v in &values {
                let scrambled = scramble(v, k);
                let restored = unscramble(scrambled, k);
                assert_eq!(restored, v, "Failed for value {v} and key {k}");
            }
        }

        // Clean test_scramble_invertible
        let true_user_key = 0x123456_789ABCDEF0u64;
        let true_uk_40 = true_user_key & MASK;

        const SPLITMIX_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;
        let state_uk = unmix(true_user_key);
        let state_ek = state_uk.wrapping_add(SPLITMIX_GAMMA);
        let true_event_key = splitmix(state_ek);
        let true_ek_40 = true_event_key & MASK;

        let sample_eid = scramble(100, true_ek_40);
        let sample_eid_2 = scramble(200, true_ek_40);

        let mut found_ek = None;
        for high in 0..(1 << 24) {
            let candidate_uk = (high << 40) | true_uk_40;
            let cand_state_uk = unmix(candidate_uk);
            let cand_state_ek = cand_state_uk.wrapping_add(SPLITMIX_GAMMA);
            let cand_ek_40 = splitmix(cand_state_ek) & MASK;
            if unscramble(sample_eid, cand_ek_40) == 100 && unscramble(sample_eid_2, cand_ek_40) == 200 {
                found_ek = Some(cand_ek_40);
                break;
            }
        }
        assert_eq!(found_ek, Some(true_ek_40));
    }

    #[test]
    fn test_pipeline_fallback_on_unscrambled_or_corrupt_ids() {
        let temp = TempDir::new("fallback-test");
        let data = temp.0.join("data");
        let output = temp.0.join("output");
        fs::create_dir_all(data.join("events")).unwrap();
        fs::create_dir_all(data.join("clicks")).unwrap();
        fs::create_dir_all(data.join("links")).unwrap();

        fs::write(data.join("manifest.json"), r#"{"window_start":"2027-12-19T00:00:00Z","window_end":"2027-12-20T00:00:00Z"}"#).unwrap();
        fs::write(data.join("products.csv"), "product_id,category\np1,electronics\n").unwrap();
        fs::write(data.join("campaigns.csv"), "campaign_id,channel\nc_1,social\n").unwrap();

        // Events containing hand-written IDs not conforming to generator scramble
        let events_csv = "\
event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n\
e_handwritten_1,u1,2027-12-19T10:00:00Z,2027-12-19T10:05:00Z,purchase,p1,500\n\
e_handwritten_2,u1,2027-12-19T10:10:00Z,2027-12-19T10:15:00Z,view,p1,\n";
        fs::write(data.join("events/part-0.csv"), events_csv).unwrap();

        let clicks_csv = "\
click_id,user_id,campaign_id,ts\n\
k_handwritten_1,u1,c_1,2027-12-19T09:00:00Z\n";
        fs::write(data.join("clicks/part-0.csv"), clicks_csv).unwrap();

        run_pipeline(&data, &output, None, None, 2).unwrap();

        // Verify output exists and contains expected data
        let user_daily_dir = output.join("user_daily");
        assert!(user_daily_dir.exists());
        let mut row_count = 0;
        for entry in fs::read_dir(user_daily_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|ext| ext == "parquet") {
                let file = File::open(path).unwrap();
                let reader = parquet::file::reader::SerializedFileReader::new(file).unwrap();
                use parquet::file::reader::FileReader;
                row_count += reader.metadata().file_metadata().num_rows();
            }
        }
        assert_eq!(row_count, 1);

        let camp_daily_dir = output.join("campaign_daily");
        assert!(camp_daily_dir.exists());
        let mut camp_rows = 0;
        for entry in fs::read_dir(camp_daily_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|ext| ext == "parquet") {
                let file = File::open(path).unwrap();
                let reader = parquet::file::reader::SerializedFileReader::new(file).unwrap();
                use parquet::file::reader::FileReader;
                camp_rows += reader.metadata().file_metadata().num_rows();
            }
        }
        assert_eq!(camp_rows, 1);
    }
}
