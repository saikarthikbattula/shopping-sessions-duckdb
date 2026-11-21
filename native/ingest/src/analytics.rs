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

use crate::identity::Identity;
use crate::parsing;
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
struct UserDailyAccumulator {
    sessions: i64,
    events: i64,
    revenue_cents: i64,
    category_revenue: HashMap<u8, i64>,
}

#[derive(Default)]
struct CampaignDailyAccumulator {
    sessions: i64,
    purchasing_sessions: i64,
    revenue_cents: i64,
    assisted_sessions: i64,
}

#[derive(Clone)]
pub struct ValidEvent {
    pub ts_ms: i64,
    pub is_purchase: bool,
    pub price_cents: i64,
    pub category_idx: u8,
}

#[derive(Clone)]
pub struct ValidClick {
    pub ts_ms: i64,
    pub click_id: String,
    pub campaign_id: String,
}

struct RawEvent {
    event_id: Box<str>,
    user_id: Option<Box<str>>,
    ingest_ms: i64,
    ts_ms: Option<i64>,
    price_cents: i64,
    category_idx: u8,
    is_purchase: bool,
    is_valid: bool,
}

struct RawClick {
    click_id: Box<str>,
    user_id: Box<str>,
    campaign_id: Box<str>,
    ts_ms: i64,
}

struct EventDedupEntry {
    ingest_ms: i64,
    user_id: Option<Box<str>>,
    ts_ms: Option<i64>,
    price_cents: i64,
    category_idx: u8,
    is_purchase: bool,
    is_valid: bool,
}

struct FastHasher(u64);
impl Hasher for FastHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0.wrapping_mul(1099511628211)) ^ u64::from(b);
        }
    }
}

#[derive(Clone, Default)]
struct FastBuildHasher;
impl BuildHasher for FastBuildHasher {
    type Hasher = FastHasher;
    #[inline]
    fn build_hasher(&self) -> FastHasher {
        FastHasher(14695981039346656037)
    }
}

const NUM_SHARDS: usize = 256;

#[inline]
fn get_shard(key: &str) -> usize {
    let mut hasher = FastBuildHasher.build_hasher();
    hasher.write(key.as_bytes());
    (hasher.finish() as usize) % NUM_SHARDS
}

fn read_csv_rows(
    path: &Path,
    count: usize,
    mut consume: impl FnMut(&[Option<&str>]) -> Result<()>,
) -> Result<()> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut buf = Vec::new();
    reader.read_until(b'\n', &mut buf)?;
    loop {
        buf.clear();
        if reader.read_until(b'\n', &mut buf)? == 0 {
            break;
        }
        let text = std::str::from_utf8(&buf)?;
        if let Some(row) = parsing::line(text, count) {
            consume(&row)?;
        }
    }
    Ok(())
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
    let mut result = Vec::new();
    for path in paths {
        let mut reader = BufReader::new(File::open(path)?);
        let length = reader.get_ref().metadata()?.len();
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
            result.push(Chunk {
                path: path.clone(),
                start,
                end,
            });
            start = end;
        }
    }
    Ok(result)
}

fn parse_chunk_rows(
    chunk: &Chunk,
    count: usize,
    cancelled: &AtomicBool,
    mut consume: impl FnMut(&[Option<&str>]) -> Result<()>,
) -> Result<()> {
    let mut reader = BufReader::new(File::open(&chunk.path)?);
    reader.seek(SeekFrom::Start(chunk.start))?;
    let mut position = chunk.start;
    let mut buffer = Vec::new();
    while position < chunk.end && !cancelled.load(Ordering::Relaxed) {
        buffer.clear();
        let read = reader.read_until(b'\n', &mut buffer)?;
        if read == 0 {
            return Err("input changed or was truncated while parsing".into());
        }
        position += read as u64;
        if let Some(row) = parsing::line(std::str::from_utf8(&buffer)?, count) {
            consume(&row)?;
        }
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
            let s = parsing::timestamp(Some(s_str)).ok_or("invalid manifest window_start")?;
            let e = parsing::timestamp(Some(e_str)).ok_or("invalid manifest window_end")?;
            (s, e)
        }
    };
    if window_start >= window_end {
        return Err("window_start must be before window_end".into());
    }

    // 2. Identity resolution from links/
    let mut ids = Identity::default();
    for file in list_csv_files(&data_dir.join("links"))? {
        read_csv_rows(&file, 2, |r| {
            ids.union(r[0], r[1]);
            Ok(())
        })?;
    }
    let ids = ids.freeze();

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

    let mut products_map: HashMap<String, u8> = HashMap::new();
    if data_dir.join("products.csv").exists() {
        read_csv_rows(&data_dir.join("products.csv"), 2, |r| {
            if let (Some(pid), Some(cat)) = (r[0], r[1]) {
                let cat_idx = get_or_insert_category(cat, &mut category_names, &mut category_to_idx);
                products_map.insert(pid.to_string(), cat_idx);
            }
            Ok(())
        })?;
    }

    // 4. Campaigns lookup: campaign_id -> channel
    let mut campaign_channels: HashMap<String, String> = HashMap::new();
    let mut valid_campaigns: HashSet<String> = HashSet::new();
    if data_dir.join("campaigns.csv").exists() {
        read_csv_rows(&data_dir.join("campaigns.csv"), 2, |r| {
            if let (Some(cid), Some(chan)) = (r[0], r[1]) {
                campaign_channels.insert(cid.to_string(), chan.to_string());
                valid_campaigns.insert(cid.to_string());
            }
            Ok(())
        })?;
    }

    // 5. Parallel Lock-Free Parsing of Events into Worker-Local Shards
    let event_files = list_csv_files(&data_dir.join("events"))?;
    let event_chunks = Arc::new(plan_chunks(&event_files, 16 * 1024 * 1024)?);
    let next_chunk = Arc::new(AtomicUsize::new(0));
    let cancelled = Arc::new(AtomicBool::new(false));

    let worker_event_shards: Vec<Vec<Vec<RawEvent>>> = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..workers_count {
            let event_chunks = Arc::clone(&event_chunks);
            let next_chunk = Arc::clone(&next_chunk);
            let cancelled = Arc::clone(&cancelled);
            let products_map = &products_map;

            handles.push(scope.spawn(move || -> Result<Vec<Vec<RawEvent>>> {
                let mut local_shards: Vec<Vec<RawEvent>> = (0..NUM_SHARDS).map(|_| Vec::new()).collect();

                while !cancelled.load(Ordering::Relaxed) {
                    let idx = next_chunk.fetch_add(1, Ordering::Relaxed);
                    let Some(chunk) = event_chunks.get(idx) else {
                        break;
                    };
                    parse_chunk_rows(chunk, 7, &cancelled, |r| {
                        let Some(event_id) = r[0] else {
                            return Ok(());
                        };
                        let ingest_ms = parsing::timestamp(r[3]).ok_or(InvalidIngest)?;
                        let ts_ms = parsing::window(r[2], i128::from(window_start), i128::from(window_end));
                        let is_purchase = r[4] == Some("purchase");
                        let price_cents = if is_purchase {
                            parsing::unsigned(r[6], 10000000).filter(|v| *v > 0)
                        } else {
                            None
                        };
                        let is_valid = r[1].is_some()
                            && ts_ms.is_some()
                            && r[5].is_some()
                            && matches!(r[4], Some("view" | "cart" | "purchase"))
                            && (!is_purchase || price_cents.is_some());

                        let category_idx = if let Some(pid) = r[5] {
                            products_map.get(pid).copied().unwrap_or(unknown_idx)
                        } else {
                            unknown_idx
                        };

                        let shard_idx = get_shard(event_id);
                        local_shards[shard_idx].push(RawEvent {
                            event_id: Box::from(event_id),
                            user_id: r[1].map(Box::from),
                            ingest_ms,
                            ts_ms,
                            price_cents: price_cents.unwrap_or(0),
                            category_idx,
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

    // 6. Lock-Free Parallel Deduplication of Events and Routing to Person Shards
    let worker_person_events: Vec<Vec<Vec<(String, ValidEvent)>>> = std::thread::scope(|scope| {
        let shards_per_worker = (NUM_SHARDS + workers_count - 1) / workers_count;
        let mut handles = Vec::new();
        let worker_event_shards = &worker_event_shards;
        let ids = &ids;

        for worker_idx in 0..workers_count {
            let start_shard = worker_idx * shards_per_worker;
            let end_shard = (start_shard + shards_per_worker).min(NUM_SHARDS);

            handles.push(scope.spawn(move || -> Vec<Vec<(String, ValidEvent)>> {
                let mut local_person_shards: Vec<Vec<(String, ValidEvent)>> =
                    (0..NUM_SHARDS).map(|_| Vec::new()).collect();

                let mut dedup_map: HashMap<Box<str>, EventDedupEntry, FastBuildHasher> =
                    HashMap::with_hasher(FastBuildHasher);

                for shard_idx in start_shard..end_shard {
                    dedup_map.clear();

                    for worker_shards in worker_event_shards {
                        for ev in &worker_shards[shard_idx] {
                            match dedup_map.get_mut(&ev.event_id) {
                                Some(existing) => {
                                    if ev.ingest_ms < existing.ingest_ms {
                                        *existing = EventDedupEntry {
                                            ingest_ms: ev.ingest_ms,
                                            user_id: ev.user_id.clone(),
                                            ts_ms: ev.ts_ms,
                                            price_cents: ev.price_cents,
                                            category_idx: ev.category_idx,
                                            is_purchase: ev.is_purchase,
                                            is_valid: ev.is_valid,
                                        };
                                    }
                                }
                                None => {
                                    dedup_map.insert(
                                        ev.event_id.clone(),
                                        EventDedupEntry {
                                            ingest_ms: ev.ingest_ms,
                                            user_id: ev.user_id.clone(),
                                            ts_ms: ev.ts_ms,
                                            price_cents: ev.price_cents,
                                            category_idx: ev.category_idx,
                                            is_purchase: ev.is_purchase,
                                            is_valid: ev.is_valid,
                                        },
                                    );
                                }
                            }
                        }
                    }

                    for entry in dedup_map.values() {
                        if entry.is_valid {
                            let ts = entry.ts_ms.unwrap();
                            if entry.ingest_ms - ts <= 86_400_000 {
                                let user = entry.user_id.as_deref().unwrap();
                                let person_id = ids.get(user).map_or_else(|| user.to_string(), String::clone);
                                let p_shard = get_shard(&person_id);
                                local_person_shards[p_shard].push((
                                    person_id,
                                    ValidEvent {
                                        ts_ms: ts,
                                        is_purchase: entry.is_purchase,
                                        price_cents: entry.price_cents,
                                        category_idx: entry.category_idx,
                                    },
                                ));
                            }
                        }
                    }
                }
                local_person_shards
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    // 7. Parallel Lock-Free Parsing of Clicks into Worker-Local Shards
    let click_files = list_csv_files(&data_dir.join("clicks"))?;
    let click_chunks = Arc::new(plan_chunks(&click_files, 16 * 1024 * 1024)?);
    let next_chunk = Arc::new(AtomicUsize::new(0));
    let cancelled = Arc::new(AtomicBool::new(false));

    let worker_click_shards: Vec<Vec<Vec<RawClick>>> = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..workers_count {
            let click_chunks = Arc::clone(&click_chunks);
            let next_chunk = Arc::clone(&next_chunk);
            let cancelled = Arc::clone(&cancelled);
            let valid_campaigns = &valid_campaigns;

            handles.push(scope.spawn(move || -> Result<Vec<Vec<RawClick>>> {
                let mut local_shards: Vec<Vec<RawClick>> = (0..NUM_SHARDS).map(|_| Vec::new()).collect();

                while !cancelled.load(Ordering::Relaxed) {
                    let idx = next_chunk.fetch_add(1, Ordering::Relaxed);
                    let Some(chunk) = click_chunks.get(idx) else {
                        break;
                    };
                    parse_chunk_rows(chunk, 4, &cancelled, |r| {
                        let Some(click_id) = r[0] else {
                            return Ok(());
                        };
                        let ts_ms = parsing::window(
                            r[3],
                            i128::from(window_start) - 604_800_000,
                            i128::from(window_end),
                        );
                        let is_valid = r[1].is_some()
                            && ts_ms.is_some()
                            && r[2].is_some_and(|c| valid_campaigns.contains(c));

                        if is_valid {
                            let shard_idx = get_shard(click_id);
                            local_shards[shard_idx].push(RawClick {
                                click_id: Box::from(click_id),
                                user_id: Box::from(r[1].unwrap()),
                                campaign_id: Box::from(r[2].unwrap()),
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

    // 8. Lock-Free Parallel Deduplication of Clicks and Routing to Person Shards
    let worker_person_clicks: Vec<Vec<Vec<(String, ValidClick)>>> = std::thread::scope(|scope| {
        let shards_per_worker = (NUM_SHARDS + workers_count - 1) / workers_count;
        let mut handles = Vec::new();
        let worker_click_shards = &worker_click_shards;
        let ids = &ids;

        for worker_idx in 0..workers_count {
            let start_shard = worker_idx * shards_per_worker;
            let end_shard = (start_shard + shards_per_worker).min(NUM_SHARDS);

            handles.push(scope.spawn(move || -> Vec<Vec<(String, ValidClick)>> {
                let mut local_person_shards: Vec<Vec<(String, ValidClick)>> =
                    (0..NUM_SHARDS).map(|_| Vec::new()).collect();

                let mut seen_clicks: HashSet<Box<str>, FastBuildHasher> = HashSet::with_hasher(FastBuildHasher);

                for shard_idx in start_shard..end_shard {
                    seen_clicks.clear();

                    for worker_shards in worker_click_shards {
                        for cl in &worker_shards[shard_idx] {
                            if seen_clicks.insert(cl.click_id.clone()) {
                                let person_id = ids.get(cl.user_id.as_ref()).map_or_else(|| cl.user_id.to_string(), String::clone);
                                let p_shard = get_shard(&person_id);
                                local_person_shards[p_shard].push((
                                    person_id,
                                    ValidClick {
                                        ts_ms: cl.ts_ms,
                                        click_id: cl.click_id.to_string(),
                                        campaign_id: cl.campaign_id.to_string(),
                                    },
                                ));
                            }
                        }
                    }
                }
                local_person_shards
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    // 9. Parallel Execution of Analytics across Person Shards
    let partial_results: Vec<(Vec<UserDailyRow>, HashMap<(String, i32), CampaignDailyAccumulator>)> =
        std::thread::scope(|scope| {
            let shards_per_worker = (NUM_SHARDS + workers_count - 1) / workers_count;
            let mut handles = Vec::new();
            let worker_person_events = &worker_person_events;
            let worker_person_clicks = &worker_person_clicks;
            let category_names = &category_names;

            for worker_idx in 0..workers_count {
                let start_shard = worker_idx * shards_per_worker;
                let end_shard = (start_shard + shards_per_worker).min(NUM_SHARDS);

                handles.push(scope.spawn(move || {
                    let mut user_rows = Vec::new();
                    let mut campaign_daily_map: HashMap<(String, i32), CampaignDailyAccumulator> = HashMap::new();

                    for shard_idx in start_shard..end_shard {
                        let mut person_events: HashMap<String, Vec<ValidEvent>> = HashMap::new();
                        let mut person_clicks: HashMap<String, Vec<ValidClick>> = HashMap::new();

                        for w_events in worker_person_events {
                            for (p, ev) in &w_events[shard_idx] {
                                person_events.entry(p.clone()).or_default().push(ev.clone());
                            }
                        }

                        for w_clicks in worker_person_clicks {
                            for (p, cl) in &w_clicks[shard_idx] {
                                person_clicks.entry(p.clone()).or_default().push(cl.clone());
                            }
                        }

                        let (u_part, c_part) = execute_shard(person_events, person_clicks, category_names);
                        user_rows.extend(u_part);
                        for (k, acc) in c_part {
                            let entry = campaign_daily_map.entry(k).or_default();
                            entry.sessions += acc.sessions;
                            entry.purchasing_sessions += acc.purchasing_sessions;
                            entry.revenue_cents += acc.revenue_cents;
                            entry.assisted_sessions += acc.assisted_sessions;
                        }
                    }
                    (user_rows, campaign_daily_map)
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

    // Combine user rows and merge campaign accumulators
    let mut total_user_rows = Vec::new();
    let mut merged_campaign_map: HashMap<(String, i32), CampaignDailyAccumulator> = HashMap::new();

    for (u_rows, c_map) in partial_results {
        total_user_rows.extend(u_rows);
        for (k, acc) in c_map {
            let entry = merged_campaign_map.entry(k).or_default();
            entry.sessions += acc.sessions;
            entry.purchasing_sessions += acc.purchasing_sessions;
            entry.revenue_cents += acc.revenue_cents;
            entry.assisted_sessions += acc.assisted_sessions;
        }
    }

    // Format CampaignDaily rows
    let mut campaign_rows = Vec::with_capacity(merged_campaign_map.len());
    for ((campaign_id, day), acc) in merged_campaign_map {
        let channel = if campaign_id == "organic" {
            "organic".to_string()
        } else {
            campaign_channels
                .get(&campaign_id)
                .cloned()
                .unwrap_or_else(|| "unknown".to_string())
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

    // 10. Write Parquet Files
    write_user_daily(output_dir, total_user_rows)?;
    write_campaign_daily(output_dir, campaign_rows)?;

    Ok(())
}

fn execute_shard(
    person_events: HashMap<String, Vec<ValidEvent>>,
    person_clicks: HashMap<String, Vec<ValidClick>>,
    category_names: &[String],
) -> (Vec<UserDailyRow>, HashMap<(String, i32), CampaignDailyAccumulator>) {
    let mut user_daily_map: HashMap<(String, i32), UserDailyAccumulator> = HashMap::new();
    let mut campaign_daily_map: HashMap<(String, i32), CampaignDailyAccumulator> = HashMap::new();

    for (person, events) in person_events {
        if events.is_empty() {
            continue;
        }

        let mut sorted_events = events;
        sorted_events.sort_unstable_by_key(|e| e.ts_ms);

        let clicks = person_clicks.get(&person);
        let mut sorted_clicks: Vec<&ValidClick> = match clicks {
            Some(c) => c.iter().collect(),
            None => Vec::new(),
        };
        sorted_clicks.sort_unstable_by(|a, b| {
            a.ts_ms
                .cmp(&b.ts_ms)
                .then_with(|| a.click_id.as_bytes().cmp(b.click_id.as_bytes()))
        });

        let mut session_start_idx = 0;
        let mut prev_ts = sorted_events[0].ts_ms;
        let mut click_left = 0usize;
        let mut click_right = 0usize;

        for i in 1..sorted_events.len() {
            let curr_ts = sorted_events[i].ts_ms;
            if curr_ts - prev_ts > 1_800_000 {
                process_session_sliding(
                    &person,
                    &sorted_events[session_start_idx..i],
                    &sorted_clicks,
                    &mut click_left,
                    &mut click_right,
                    &mut user_daily_map,
                    &mut campaign_daily_map,
                );
                session_start_idx = i;
            }
            prev_ts = curr_ts;
        }
        process_session_sliding(
            &person,
            &sorted_events[session_start_idx..],
            &sorted_clicks,
            &mut click_left,
            &mut click_right,
            &mut user_daily_map,
            &mut campaign_daily_map,
        );
    }

    let mut user_rows = Vec::with_capacity(user_daily_map.len());
    for ((user_id, day), acc) in user_daily_map {
        let top_category = if acc.category_revenue.is_empty() {
            None
        } else {
            acc.category_revenue
                .iter()
                .max_by(|(cat_idx_a, rev_a), (cat_idx_b, rev_b)| {
                    let name_a = &category_names[**cat_idx_a as usize];
                    let name_b = &category_names[**cat_idx_b as usize];
                    rev_a
                        .cmp(rev_b)
                        .then_with(|| name_b.as_bytes().cmp(name_a.as_bytes()))
                })
                .map(|(cat_idx, _)| category_names[*cat_idx as usize].clone())
        };
        user_rows.push(UserDailyRow {
            user_id,
            day,
            sessions: acc.sessions,
            events: acc.events,
            revenue_cents: acc.revenue_cents,
            top_category,
        });
    }

    (user_rows, campaign_daily_map)
}

#[inline]
fn process_session_sliding(
    person: &str,
    session_events: &[ValidEvent],
    sorted_clicks: &[&ValidClick],
    click_left: &mut usize,
    click_right: &mut usize,
    user_daily_map: &mut HashMap<(String, i32), UserDailyAccumulator>,
    campaign_daily_map: &mut HashMap<(String, i32), CampaignDailyAccumulator>,
) {
    let start_ms = session_events[0].ts_ms;
    let day = epoch_days(start_ms);

    let user_entry = user_daily_map
        .entry((person.to_string(), day))
        .or_default();
    user_entry.sessions += 1;

    let mut session_revenue = 0i64;
    let mut session_purchases = 0i64;

    for ev in session_events {
        if ev.is_purchase {
            session_purchases += 1;
            session_revenue += ev.price_cents;
            *user_entry.category_revenue.entry(ev.category_idx).or_default() += ev.price_cents;
        }
    }
    user_entry.events += session_events.len() as i64;
    user_entry.revenue_cents += session_revenue;

    // Sliding window boundaries:
    // eligible clicks have: start_ms - 604_800_000 <= ts_ms <= start_ms
    let window_start = start_ms.saturating_sub(604_800_000);

    while *click_right < sorted_clicks.len() && sorted_clicks[*click_right].ts_ms <= start_ms {
        *click_right += 1;
    }
    while *click_left < *click_right && sorted_clicks[*click_left].ts_ms < window_start {
        *click_left += 1;
    }

    let left = *click_left;
    let right = *click_right;

    let winner = if left < right {
        let max_ts = sorted_clicks[right - 1].ts_ms;
        let mut best = sorted_clicks[right - 1];
        let mut idx = right - 1;
        while idx > left && sorted_clicks[idx - 1].ts_ms == max_ts {
            idx -= 1;
            if sorted_clicks[idx].click_id.as_bytes() < best.click_id.as_bytes() {
                best = sorted_clicks[idx];
            }
        }
        Some(best)
    } else {
        None
    };

    let winning_campaign = winner.map_or("organic", |w| w.campaign_id.as_str());

    let credited_entry = campaign_daily_map
        .entry((winning_campaign.to_string(), day))
        .or_default();
    credited_entry.sessions += 1;
    if session_purchases > 0 {
        credited_entry.purchasing_sessions += 1;
    }
    credited_entry.revenue_cents += session_revenue;

    if left < right {
        let count = right - left;
        if count <= 8 {
            let mut seen: [&str; 8] = [""; 8];
            let mut seen_len = 0;
            for i in left..right {
                let camp = sorted_clicks[i].campaign_id.as_str();
                if camp != "organic" && !seen[..seen_len].contains(&camp) {
                    seen[seen_len] = camp;
                    seen_len += 1;
                    let assist_entry = campaign_daily_map
                        .entry((camp.to_string(), day))
                        .or_default();
                    assist_entry.assisted_sessions += 1;
                }
            }
        } else {
            let mut seen: HashSet<&str> = HashSet::new();
            for i in left..right {
                let camp = sorted_clicks[i].campaign_id.as_str();
                if camp != "organic" && seen.insert(camp) {
                    let assist_entry = campaign_daily_map
                        .entry((camp.to_string(), day))
                        .or_default();
                    assist_entry.assisted_sessions += 1;
                }
            }
        }
    }
}

pub fn write_user_daily(output_dir: &Path, rows: Vec<UserDailyRow>) -> Result<()> {
    let out_dir = output_dir.join("user_daily");
    fs::create_dir_all(&out_dir)?;
    let file = File::create(out_dir.join("part-0.parquet"))?;

    let schema = user_daily_schema();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let mut user_builder = StringBuilder::new();
    let mut day_builder = Date32Builder::new();
    let mut sess_builder = Int64Builder::new();
    let mut ev_builder = Int64Builder::new();
    let mut rev_builder = Int64Builder::new();
    let mut top_cat_builder = StringBuilder::new();

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
