use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::hash::{BuildHasher, Hasher};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
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
    category_revenue: HashMap<String, i64>,
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
    pub category: String,
}

#[derive(Clone)]
pub struct ValidClick {
    pub person_id: String,
    pub ts_ms: i64,
    pub click_id: String,
    pub campaign_id: String,
}

struct EventCandidate {
    ingest_ms: i64,
    user_id: Option<String>,
    ts_ms: Option<i64>,
    is_purchase: bool,
    product_id: Option<String>,
    price_cents: i64,
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
            self.0 = self.0.wrapping_mul(1099511628211) ^ u64::from(b);
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

const NUM_SHARDS: usize = 64;

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
    let workers_count = workers_count.max(1);

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

    // 3. Products lookup: product_id -> category
    let mut products_map: HashMap<String, String> = HashMap::new();
    if data_dir.join("products.csv").exists() {
        read_csv_rows(&data_dir.join("products.csv"), 2, |r| {
            if let (Some(pid), Some(cat)) = (r[0], r[1]) {
                products_map.insert(pid.to_string(), cat.to_string());
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

    // 5. Parallel Parsing & Deduplication of Events
    let event_files = list_csv_files(&data_dir.join("events"))?;
    let event_chunks = Arc::new(plan_chunks(&event_files, 16 * 1024 * 1024)?);

    let event_shards: Arc<Vec<Mutex<HashMap<String, EventCandidate, FastBuildHasher>>>> =
        Arc::new((0..NUM_SHARDS).map(|_| Mutex::new(HashMap::with_hasher(FastBuildHasher))).collect());

    let cancelled = Arc::new(AtomicBool::new(false));
    let next_chunk = Arc::new(AtomicUsize::new(0));

    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..workers_count {
            let event_chunks = Arc::clone(&event_chunks);
            let event_shards = Arc::clone(&event_shards);
            let next_chunk = Arc::clone(&next_chunk);
            let cancelled = Arc::clone(&cancelled);

            handles.push(scope.spawn(move || -> Result<()> {
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

                        let shard_idx = get_shard(event_id);
                        let mut shard = event_shards[shard_idx].lock().unwrap();

                        match shard.get_mut(event_id) {
                            Some(existing) => {
                                if ingest_ms < existing.ingest_ms {
                                    *existing = EventCandidate {
                                        ingest_ms,
                                        user_id: r[1].map(str::to_string),
                                        ts_ms,
                                        is_purchase,
                                        product_id: r[5].map(str::to_string),
                                        price_cents: price_cents.unwrap_or(0),
                                        is_valid,
                                    };
                                }
                            }
                            None => {
                                shard.insert(
                                    event_id.to_string(),
                                    EventCandidate {
                                        ingest_ms,
                                        user_id: r[1].map(str::to_string),
                                        ts_ms,
                                        is_purchase,
                                        product_id: r[5].map(str::to_string),
                                        price_cents: price_cents.unwrap_or(0),
                                        is_valid,
                                    },
                                );
                            }
                        }
                        Ok(())
                    })?;
                }
                Ok(())
            }));
        }
        for h in handles {
            h.join().unwrap()?;
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })?;

    // Parallel extraction of valid events partitioned by person_shard
    let person_partitions: Arc<Vec<Mutex<HashMap<String, Vec<ValidEvent>>>>> =
        Arc::new((0..workers_count).map(|_| Mutex::new(HashMap::new())).collect());

    std::thread::scope(|scope| {
        let shards_per_worker = (NUM_SHARDS + workers_count - 1) / workers_count;
        let mut handles = Vec::new();
        for worker_idx in 0..workers_count {
            let start_shard = worker_idx * shards_per_worker;
            let end_shard = (start_shard + shards_per_worker).min(NUM_SHARDS);
            let event_shards = &event_shards;
            let ids = &ids;
            let products_map = &products_map;
            let person_partitions = Arc::clone(&person_partitions);

            handles.push(scope.spawn(move || {
                let mut local_parts: Vec<HashMap<String, Vec<ValidEvent>>> =
                    (0..workers_count).map(|_| HashMap::new()).collect();

                for shard_idx in start_shard..end_shard {
                    let mut shard = event_shards[shard_idx].lock().unwrap();
                    for (_, candidate) in shard.drain() {
                        if candidate.is_valid {
                            if let (Some(user), Some(ts)) = (candidate.user_id, candidate.ts_ms) {
                                if candidate.ingest_ms <= ts + 86_400_000 {
                                    let person_id = ids.get(&user).map_or(user, String::clone);
                                    let category = match candidate.product_id {
                                        Some(ref pid) => products_map
                                            .get(pid)
                                            .cloned()
                                            .unwrap_or_else(|| "unknown".to_string()),
                                        None => "unknown".to_string(),
                                    };
                                    let part_idx = get_shard(&person_id) % workers_count;
                                    local_parts[part_idx]
                                        .entry(person_id)
                                        .or_default()
                                        .push(ValidEvent {
                                            ts_ms: ts,
                                            is_purchase: candidate.is_purchase,
                                            price_cents: candidate.price_cents,
                                            category,
                                        });
                                }
                            }
                        }
                    }
                }

                for (p_idx, map) in local_parts.into_iter().enumerate() {
                    if !map.is_empty() {
                        let mut target = person_partitions[p_idx].lock().unwrap();
                        for (k, v) in map {
                            target.entry(k).or_default().extend(v);
                        }
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });

    // 6. Parallel Parsing & Deduplication of Clicks
    let click_files = list_csv_files(&data_dir.join("clicks"))?;
    let click_chunks = Arc::new(plan_chunks(&click_files, 16 * 1024 * 1024)?);

    let click_shards: Arc<Vec<Mutex<HashMap<String, ValidClick, FastBuildHasher>>>> =
        Arc::new((0..NUM_SHARDS).map(|_| Mutex::new(HashMap::with_hasher(FastBuildHasher))).collect());

    let cancelled = Arc::new(AtomicBool::new(false));
    let next_chunk = Arc::new(AtomicUsize::new(0));

    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..workers_count {
            let click_chunks = Arc::clone(&click_chunks);
            let click_shards = Arc::clone(&click_shards);
            let next_chunk = Arc::clone(&next_chunk);
            let cancelled = Arc::clone(&cancelled);
            let valid_campaigns = &valid_campaigns;
            let ids = &ids;

            handles.push(scope.spawn(move || -> Result<()> {
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
                            let mut shard = click_shards[shard_idx].lock().unwrap();
                            if !shard.contains_key(click_id) {
                                let user = r[1].unwrap();
                                let person_id = ids.get(user).map_or_else(|| user.to_string(), String::clone);
                                shard.insert(
                                    click_id.to_string(),
                                    ValidClick {
                                        person_id,
                                        ts_ms: ts_ms.unwrap(),
                                        click_id: click_id.to_string(),
                                        campaign_id: r[2].unwrap().to_string(),
                                    },
                                );
                            }
                        }
                        Ok(())
                    })?;
                }
                Ok(())
            }));
        }
        for h in handles {
            h.join().unwrap()?;
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })?;

    // Group clicks partitioned by person_shard
    let click_partitions: Arc<Vec<Mutex<HashMap<String, Vec<ValidClick>>>>> =
        Arc::new((0..workers_count).map(|_| Mutex::new(HashMap::new())).collect());

    std::thread::scope(|scope| {
        let shards_per_worker = (NUM_SHARDS + workers_count - 1) / workers_count;
        let mut handles = Vec::new();
        for worker_idx in 0..workers_count {
            let start_shard = worker_idx * shards_per_worker;
            let end_shard = (start_shard + shards_per_worker).min(NUM_SHARDS);
            let click_shards = &click_shards;
            let click_partitions = Arc::clone(&click_partitions);

            handles.push(scope.spawn(move || {
                let mut local_parts: Vec<HashMap<String, Vec<ValidClick>>> =
                    (0..workers_count).map(|_| HashMap::new()).collect();

                for shard_idx in start_shard..end_shard {
                    let mut shard = click_shards[shard_idx].lock().unwrap();
                    for (_, click) in shard.drain() {
                        let part_idx = get_shard(&click.person_id) % workers_count;
                        local_parts[part_idx]
                            .entry(click.person_id.clone())
                            .or_default()
                            .push(click);
                    }
                }

                for (p_idx, map) in local_parts.into_iter().enumerate() {
                    if !map.is_empty() {
                        let mut target = click_partitions[p_idx].lock().unwrap();
                        for (k, v) in map {
                            target.entry(k).or_default().extend(v);
                        }
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });

    // 7. Parallel Execution of Analytics across partitions
    let partial_results: Vec<(Vec<UserDailyRow>, HashMap<(String, i32), CampaignDailyAccumulator>)> =
        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for worker_idx in 0..workers_count {
                let person_partitions = &person_partitions;
                let click_partitions = &click_partitions;

                handles.push(scope.spawn(move || {
                    let mut events_map = person_partitions[worker_idx].lock().unwrap();
                    let mut clicks_map = click_partitions[worker_idx].lock().unwrap();

                    let events = std::mem::take(&mut *events_map);
                    let clicks = std::mem::take(&mut *clicks_map);

                    execute_partition(events, clicks)
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

    // 8. Write Parquet Files
    write_user_daily(output_dir, total_user_rows)?;
    write_campaign_daily(output_dir, campaign_rows)?;

    Ok(())
}

fn execute_partition(
    person_events: HashMap<String, Vec<ValidEvent>>,
    person_clicks: HashMap<String, Vec<ValidClick>>,
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

        for i in 1..sorted_events.len() {
            let curr_ts = sorted_events[i].ts_ms;
            if curr_ts - prev_ts > 1_800_000 {
                process_session(
                    &person,
                    &sorted_events[session_start_idx..i],
                    &sorted_clicks,
                    &mut user_daily_map,
                    &mut campaign_daily_map,
                );
                session_start_idx = i;
            }
            prev_ts = curr_ts;
        }
        process_session(
            &person,
            &sorted_events[session_start_idx..],
            &sorted_clicks,
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
                .max_by(|(cat_a, rev_a), (cat_b, rev_b)| {
                    rev_a
                        .cmp(rev_b)
                        .then_with(|| cat_b.as_bytes().cmp(cat_a.as_bytes()))
                })
                .map(|(cat, _)| cat.clone())
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

fn process_session(
    person: &str,
    session_events: &[ValidEvent],
    sorted_clicks: &[&ValidClick],
    user_daily_map: &mut HashMap<(String, i32), UserDailyAccumulator>,
    campaign_daily_map: &mut HashMap<(String, i32), CampaignDailyAccumulator>,
) {
    let start_ms = session_events[0].ts_ms;
    let day = epoch_days(start_ms);

    let mut session_events_count = 0i64;
    let mut session_purchases_count = 0i64;
    let mut session_revenue = 0i64;

    let user_entry = user_daily_map
        .entry((person.to_string(), day))
        .or_default();
    user_entry.sessions += 1;

    for ev in session_events {
        session_events_count += 1;
        if ev.is_purchase {
            session_purchases_count += 1;
            session_revenue += ev.price_cents;
            *user_entry
                .category_revenue
                .entry(ev.category.clone())
                .or_default() += ev.price_cents;
        }
    }
    user_entry.events += session_events_count;
    user_entry.revenue_cents += session_revenue;

    let window_start = start_ms.saturating_sub(604_800_000);
    let left_idx = sorted_clicks.partition_point(|c| c.ts_ms < window_start);
    let right_idx = sorted_clicks.partition_point(|c| c.ts_ms <= start_ms);

    let eligible_clicks = if left_idx < right_idx {
        &sorted_clicks[left_idx..right_idx]
    } else {
        &[]
    };

    let winner = if !eligible_clicks.is_empty() {
        let max_ts = eligible_clicks.last().unwrap().ts_ms;
        let first_with_max_ts_idx = eligible_clicks.partition_point(|c| c.ts_ms < max_ts);
        Some(eligible_clicks[first_with_max_ts_idx])
    } else {
        None
    };

    let winning_campaign = winner.map_or("organic", |w| w.campaign_id.as_str());

    let credited_entry = campaign_daily_map
        .entry((winning_campaign.to_string(), day))
        .or_default();
    credited_entry.sessions += 1;
    if session_purchases_count > 0 {
        credited_entry.purchasing_sessions += 1;
    }
    credited_entry.revenue_cents += session_revenue;

    if !eligible_clicks.is_empty() {
        let mut seen_campaigns: HashSet<&str> = HashSet::new();
        for click in eligible_clicks {
            if click.campaign_id != "organic" && seen_campaigns.insert(&click.campaign_id) {
                let assist_entry = campaign_daily_map
                    .entry((click.campaign_id.clone(), day))
                    .or_default();
                assist_entry.assisted_sessions += 1;
            }
        }
    }
}

pub fn write_user_daily(output_dir: &Path, mut rows: Vec<UserDailyRow>) -> Result<()> {
    let folder = output_dir.join("user_daily");
    fs::create_dir_all(&folder)?;
    let path = folder.join("part-0.parquet");

    rows.sort_unstable_by(|a, b| a.user_id.cmp(&b.user_id).then_with(|| a.day.cmp(&b.day)));

    let mut user_id_builder = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
    let mut day_builder = Date32Builder::with_capacity(rows.len());
    let mut sessions_builder = Int64Builder::with_capacity(rows.len());
    let mut events_builder = Int64Builder::with_capacity(rows.len());
    let mut revenue_builder = Int64Builder::with_capacity(rows.len());
    let mut top_cat_builder = StringBuilder::with_capacity(rows.len(), rows.len() * 16);

    for r in rows {
        user_id_builder.append_value(&r.user_id);
        day_builder.append_value(r.day);
        sessions_builder.append_value(r.sessions);
        events_builder.append_value(r.events);
        revenue_builder.append_value(r.revenue_cents);
        match r.top_category {
            Some(ref cat) => top_cat_builder.append_value(cat),
            None => top_cat_builder.append_null(),
        }
    }

    let schema = user_daily_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(user_id_builder.finish()),
            Arc::new(day_builder.finish()),
            Arc::new(sessions_builder.finish()),
            Arc::new(events_builder.finish()),
            Arc::new(revenue_builder.finish()),
            Arc::new(top_cat_builder.finish()),
        ],
    )?;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(File::create(path)?, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

pub fn write_campaign_daily(output_dir: &Path, mut rows: Vec<CampaignDailyRow>) -> Result<()> {
    let folder = output_dir.join("campaign_daily");
    fs::create_dir_all(&folder)?;
    let path = folder.join("part-0.parquet");

    rows.sort_unstable_by(|a, b| {
        a.campaign_id
            .cmp(&b.campaign_id)
            .then_with(|| a.day.cmp(&b.day))
    });

    let mut campaign_id_builder = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
    let mut channel_builder = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
    let mut day_builder = Date32Builder::with_capacity(rows.len());
    let mut sessions_builder = Int64Builder::with_capacity(rows.len());
    let mut purchasing_builder = Int64Builder::with_capacity(rows.len());
    let mut revenue_builder = Int64Builder::with_capacity(rows.len());
    let mut assisted_builder = Int64Builder::with_capacity(rows.len());

    for r in rows {
        campaign_id_builder.append_value(&r.campaign_id);
        channel_builder.append_value(&r.channel);
        day_builder.append_value(r.day);
        sessions_builder.append_value(r.sessions);
        purchasing_builder.append_value(r.purchasing_sessions);
        revenue_builder.append_value(r.revenue_cents);
        assisted_builder.append_value(r.assisted_sessions);
    }

    let schema = campaign_daily_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(campaign_id_builder.finish()),
            Arc::new(channel_builder.finish()),
            Arc::new(day_builder.finish()),
            Arc::new(sessions_builder.finish()),
            Arc::new(purchasing_builder.finish()),
            Arc::new(revenue_builder.finish()),
            Arc::new(assisted_builder.finish()),
        ],
    )?;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(File::create(path)?, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
