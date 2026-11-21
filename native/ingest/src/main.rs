mod analytics;
mod identity;
mod parsing;
use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fs::{self, File},
    io::{BufRead, BufReader, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const MAX_ROWS: usize = 65536;
const MAX_BYTES: usize = 16 * 1024 * 1024;
#[derive(Debug)]
struct InvalidIngest;
impl std::fmt::Display for InvalidIngest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ingest_ts violates the guaranteed valid timestamp input")
    }
}
impl Error for InvalidIngest {}

fn schema(table: &str) -> Arc<Schema> {
    use DataType::{Boolean, Int64, Utf8};
    let fields = match table {
        "events" => vec![
            ("event_id", Utf8, false),
            ("person_id", Utf8, true),
            ("ingest_ms", Int64, false),
            ("ts_ms", Int64, true),
            ("event_type", Utf8, true),
            ("product_id", Utf8, true),
            ("price_cents", Int64, true),
            ("payload_valid", Boolean, false),
        ],
        "clicks" => vec![
            ("click_id", Utf8, false),
            ("person_id", Utf8, true),
            ("campaign_id", Utf8, true),
            ("ts_ms", Int64, true),
            ("payload_valid", Boolean, false),
        ],
        "products" => vec![("product_id", Utf8, false), ("category", Utf8, false)],
        "campaigns" => vec![("campaign_id", Utf8, false), ("channel", Utf8, false)],
        _ => unreachable!(),
    };
    Arc::new(Schema::new(
        fields
            .into_iter()
            .map(|(n, t, nullable)| Field::new(n, t, nullable))
            .collect::<Vec<_>>(),
    ))
}
enum Column {
    S(Vec<Option<String>>),
    I(Vec<Option<i64>>),
    B(Vec<bool>),
}
enum Value<'a> {
    S(Option<&'a str>),
    I(Option<i64>),
    B(bool),
}
struct Sink {
    writer: ArrowWriter<File>,
    schema: Arc<Schema>,
    columns: Vec<Column>,
    rows: usize,
    bytes: usize,
    total: u64,
    limit: usize,
}
impl Sink {
    fn columns(schema: &Schema) -> Vec<Column> {
        schema
            .fields()
            .iter()
            .map(|f| match f.data_type() {
                DataType::Utf8 => Column::S(Vec::new()),
                DataType::Int64 => Column::I(Vec::new()),
                DataType::Boolean => Column::B(Vec::new()),
                _ => unreachable!(),
            })
            .collect()
    }
    fn new(path: &Path, table: &str, limit: usize) -> Result<Self> {
        let schema = schema(table);
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_size(limit)
            .build();
        let writer = ArrowWriter::try_new(File::create(path)?, schema.clone(), Some(props))?;
        let columns = Self::columns(&schema);
        Ok(Self {
            writer,
            schema,
            columns,
            rows: 0,
            bytes: 0,
            total: 0,
            limit,
        })
    }
    fn push(&mut self, values: &[Value<'_>]) -> Result<()> {
        for (col, v) in self.columns.iter_mut().zip(values) {
            match (col, v) {
                (Column::S(c), Value::S(v)) => {
                    self.bytes += v.map_or(0, str::len) + 32;
                    c.push(v.map(str::to_owned));
                }
                (Column::I(c), Value::I(v)) => {
                    self.bytes += 16;
                    c.push(*v);
                }
                (Column::B(c), Value::B(v)) => {
                    self.bytes += 1;
                    c.push(*v);
                }
                _ => unreachable!(),
            }
        }
        self.rows += 1;
        self.total += 1;
        if self.rows >= self.limit || self.bytes >= MAX_BYTES {
            self.flush()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        if self.rows == 0 {
            return Ok(());
        }
        let cols = std::mem::replace(&mut self.columns, Self::columns(&self.schema));
        let arrays: Vec<ArrayRef> = cols
            .into_iter()
            .map(|c| match c {
                Column::S(v) => {
                    Arc::new(StringArray::from_iter(v.iter().map(|s| s.as_deref()))) as ArrayRef
                }
                Column::I(v) => Arc::new(Int64Array::from(v)) as ArrayRef,
                Column::B(v) => Arc::new(BooleanArray::from(v)) as ArrayRef,
            })
            .collect();
        self.writer
            .write(&RecordBatch::try_new(self.schema.clone(), arrays)?)?;
        self.writer.flush()?;
        self.rows = 0;
        self.bytes = 0;
        Ok(())
    }
    fn close(mut self) -> Result<u64> {
        self.flush()?;
        self.writer.close()?;
        Ok(self.total)
    }
}
fn rows(
    path: &Path,
    count: usize,
    mut consume: impl FnMut(&[Option<&str>]) -> Result<()>,
) -> Result<()> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut buf = Vec::new();
    reader.read_until(b'\n', &mut buf)?; // Exactly one physical header; no newline translation.
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
fn parts(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    // Python glob treats an absent table directory as empty.
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
fn person<'a>(ids: &'a HashMap<String, String>, user: Option<&'a str>) -> Option<&'a str> {
    user.map(|u| ids.get(u).map_or(u, String::as_str))
}
struct Args {
    data: PathBuf,
    stage: PathBuf,
    start: i64,
    end: i64,
    batch: usize,
    workers: usize,
}
enum Mode {
    Stage(Args),
    Pipeline {
        data: PathBuf,
        output: PathBuf,
        start: Option<i64>,
        end: Option<i64>,
        workers: usize,
    },
}
fn cli_mode() -> std::result::Result<Mode, String> {
    let raw_args: Vec<String> = std::env::args().skip(1).collect();
    if raw_args.is_empty() {
        let data_env = std::env::var("DATA_DIR").ok();
        let out_env = std::env::var("OUTPUT_DIR").ok();
        if let (Some(d), Some(o)) = (data_env, out_env) {
            let workers = std::env::var("NCPU")
                .ok()
                .and_then(|w| w.parse::<usize>().ok())
                .unwrap_or(8)
                .max(1);
            return Ok(Mode::Pipeline {
                data: PathBuf::from(d),
                output: PathBuf::from(o),
                start: None,
                end: None,
                workers,
            });
        }
        return Err("missing arguments".into());
    }

    if raw_args.len() == 2 && !raw_args[0].starts_with('-') && !raw_args[1].starts_with('-') {
        let workers = std::env::var("NCPU")
            .ok()
            .and_then(|w| w.parse::<usize>().ok())
            .unwrap_or(8)
            .max(1);
        return Ok(Mode::Pipeline {
            data: PathBuf::from(&raw_args[0]),
            output: PathBuf::from(&raw_args[1]),
            start: None,
            end: None,
            workers,
        });
    }

    let mut values = HashMap::new();
    let mut it = raw_args.into_iter();
    while let Some(key) = it.next() {
        if ![
            "--data-dir",
            "--stage-dir",
            "--output-dir",
            "--scratch-dir",
            "--window-start-ms",
            "--window-end-ms",
            "--batch-rows",
            "--workers",
        ]
        .contains(&key.as_str())
        {
            return Err(format!("unknown option {key}"));
        }
        let value = it
            .next()
            .ok_or_else(|| format!("missing value for {key}"))?;
        if values.insert(key, value).is_some() {
            return Err("duplicate option".into());
        }
    }

    let workers = values
        .get("--workers")
        .map_or(Ok(8), |v| v.parse::<usize>())
        .map_err(|e| e.to_string())?;
    if workers == 0 {
        return Err("invalid workers count".into());
    }

    if let Some(stage_str) = values.get("--stage-dir") {
        let get = |k: &str| values.get(k).ok_or_else(|| format!("missing {k}"));
        let start = get("--window-start-ms")?
            .parse::<i64>()
            .map_err(|e| e.to_string())?;
        let end = get("--window-end-ms")?
            .parse::<i64>()
            .map_err(|e| e.to_string())?;
        let batch = get("--batch-rows")?
            .parse::<u64>()
            .map_err(|e| e.to_string())?;
        if start >= end || batch == 0 {
            return Err("invalid window or batch size".into());
        }
        return Ok(Mode::Stage(Args {
            data: PathBuf::from(get("--data-dir")?),
            stage: PathBuf::from(stage_str),
            start,
            end,
            batch: batch.min(MAX_ROWS as u64) as usize,
            workers,
        }));
    }

    if let Some(output_str) = values.get("--output-dir") {
        let data_str = values.get("--data-dir").ok_or_else(|| "missing --data-dir".to_string())?;
        let start = values.get("--window-start-ms").and_then(|v| v.parse::<i64>().ok());
        let end = values.get("--window-end-ms").and_then(|v| v.parse::<i64>().ok());
        return Ok(Mode::Pipeline {
            data: PathBuf::from(data_str),
            output: PathBuf::from(output_str),
            start,
            end,
            workers,
        });
    }

    Err("either --stage-dir or --output-dir is required".into())
}
/// Every descriptor starts and ends at a physical record boundary. Planning reads
/// only each header and the suffix after a target offset, not entire input files.
#[derive(Debug)]
struct Chunk {
    path: PathBuf,
    start: u64,
    end: u64,
}
fn chunks(paths: &[PathBuf], size: u64) -> Result<Vec<Chunk>> {
    assert!(size > 0);
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
fn chunk_rows(
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
/// Catch panics inside the worker so cancellation is visible immediately. Join
/// every worker before returning any error, including invalid-ingest errors.
fn workers<T: Send>(
    count: usize,
    work: impl Fn(usize, &AtomicBool) -> Result<T> + Sync,
) -> Result<Vec<T>> {
    let cancelled = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..count)
            .map(|worker| {
                let work = &work;
                let cancelled = &cancelled;
                let handle = std::thread::Builder::new()
                    .name(format!("ingest-{worker}"))
                    .spawn_scoped(scope, move || {
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            work(worker, cancelled)
                        }))
                        .unwrap_or_else(|_| Err("native ingestion worker panicked".into()));
                        if result.is_err() {
                            cancelled.store(true, Ordering::Relaxed);
                        }
                        result
                    });
                if handle.is_err() {
                    cancelled.store(true, Ordering::Relaxed);
                }
                handle
            })
            .collect::<Vec<_>>();
        let mut results = Vec::new();
        let mut error: Option<Box<dyn Error + Send + Sync>> = None;
        for handle in handles {
            let result = match handle {
                Ok(handle) => handle
                    .join()
                    .unwrap_or_else(|_| Err("native ingestion worker panicked".into())),
                Err(e) => {
                    cancelled.store(true, Ordering::Relaxed);
                    Err(e.into())
                }
            };
            match result {
                Ok(value) => results.push(value),
                Err(e) => {
                    if error.is_none() || e.downcast_ref::<InvalidIngest>().is_some() {
                        error = Some(e);
                    }
                }
            }
        }
        error.map_or(Ok(results), Err)
    })
}
struct Part {
    name: String,
    rows: u64,
}
struct Context<'a> {
    ids: &'a HashMap<String, String>,
    campaigns: &'a HashSet<String>,
    null_campaign: bool,
    start: i64,
    end: i64,
}
fn stage_table(args: &Args, table: &str, context: &Context<'_>) -> Result<Vec<Part>> {
    stage_table_with_chunk_size(args, table, context, MAX_BYTES as u64)
}
fn stage_table_with_chunk_size(
    args: &Args,
    table: &str,
    context: &Context<'_>,
    chunk_size: u64,
) -> Result<Vec<Part>> {
    let chunks = chunks(&parts(&args.data.join(table))?, chunk_size)?;
    let next = AtomicUsize::new(0);
    let count = args.workers.min(chunks.len().max(1));
    workers(count, |worker, cancelled| {
        let name = format!("part-{worker}.parquet");
        let mut sink = Sink::new(&args.stage.join(table).join(&name), table, args.batch)?;
        while !cancelled.load(Ordering::Relaxed) {
            let index = next.fetch_add(1, Ordering::Relaxed);
            let Some(chunk) = chunks.get(index) else {
                break;
            };
            chunk_rows(
                chunk,
                if table == "events" { 7 } else { 4 },
                cancelled,
                |row| process_row(table, context, &mut sink, row),
            )?;
        }
        Ok(Part {
            name,
            rows: sink.close()?,
        })
    })
}
fn process_row(
    table: &str,
    context: &Context<'_>,
    sink: &mut Sink,
    r: &[Option<&str>],
) -> Result<()> {
    if table == "events" {
        if r[0].is_none() {
            return Ok(());
        }
        let ts = parsing::window(r[2], i128::from(context.start), i128::from(context.end));
        let ingest = parsing::timestamp(r[3]).ok_or(InvalidIngest)?;
        let cents = if r[4] == Some("purchase") {
            parsing::unsigned(r[6], 10000000).filter(|v| *v > 0)
        } else {
            None
        };
        let valid = r[1].is_some()
            && ts.is_some()
            && r[5].is_some()
            && matches!(r[4], Some("view" | "cart" | "purchase"))
            && (r[4] != Some("purchase") || cents.is_some());
        sink.push(&[
            Value::S(r[0]),
            Value::S(person(context.ids, r[1])),
            Value::I(Some(ingest)),
            Value::I(ts),
            Value::S(r[4]),
            Value::S(r[5]),
            Value::I(cents),
            Value::B(valid),
        ])
    } else {
        if r[0].is_none() {
            return Ok(());
        }
        let ts = parsing::window(
            r[3],
            i128::from(context.start) - 604800000,
            i128::from(context.end),
        );
        let valid = r[1].is_some()
            && ts.is_some()
            && r[2].map_or(context.null_campaign, |id| context.campaigns.contains(id));
        sink.push(&[
            Value::S(r[0]),
            Value::S(person(context.ids, r[1])),
            Value::S(r[2]),
            Value::I(ts),
            Value::B(valid),
        ])
    }
}

fn run(a: Args) -> Result<()> {
    if fs::read_dir(&a.stage)?.next().is_some() {
        return Err("stage directory must be empty".into());
    }
    let mut ids = identity::Identity::default();
    for file in parts(&a.data.join("links"))? {
        rows(&file, 2, |r| {
            ids.union(r[0], r[1]);
            Ok(())
        })?;
    }
    let ids = ids.freeze();
    fs::create_dir(a.stage.join("events"))?;
    fs::create_dir(a.stage.join("clicks"))?;
    let mut product = Sink::new(&a.stage.join("products.parquet"), "products", a.batch)?;
    rows(&a.data.join("products.csv"), 2, |r| {
        product.push(&[Value::S(r[0]), Value::S(r[1])])
    })?;
    let products = product.close()?;
    let mut campaigns: HashSet<String> = HashSet::new();
    let mut null_campaign = false;
    let mut campaign = Sink::new(&a.stage.join("campaigns.parquet"), "campaigns", a.batch)?;
    rows(&a.data.join("campaigns.csv"), 2, |r| {
        if let Some(id) = r[0] {
            campaigns.insert(id.to_owned());
        } else {
            null_campaign = true;
        }
        campaign.push(&[Value::S(r[0]), Value::S(r[1])])
    })?;
    let campaign_count = campaign.close()?;
    let context = Context {
        ids: &ids,
        campaigns: &campaigns,
        null_campaign,
        start: a.start,
        end: a.end,
    };
    let event_parts = stage_table(&a, "events", &context)?;
    let click_parts = stage_table(&a, "clicks", &context)?;
    let events: u64 = event_parts.iter().map(|part| part.rows).sum();
    let clicks: u64 = click_parts.iter().map(|part| part.rows).sum();
    let part_json = |parts: &[Part]| {
        parts
            .iter()
            .map(|p| serde_json::json!({"name": p.name, "rows": p.rows}))
            .collect::<Vec<_>>()
    };
    let marker = serde_json::json!({"protocol_version":2,"window_start_ms":a.start,"window_end_ms":a.end,"rows":{"events":events,"clicks":clicks,"products":products,"campaigns":campaign_count},"parts":{"events":part_json(&event_parts),"clicks":part_json(&click_parts)}});
    let temp = a.stage.join("_SUCCESS.json.tmp");
    let mut f = File::create(&temp)?;
    serde_json::to_writer(&mut f, &marker)?;
    f.write_all(b"\n")?;
    f.sync_all()?;
    drop(f);
    fs::rename(temp, a.stage.join("_SUCCESS.json"))?;
    Ok(())
}
fn main() {
    let mode = match cli_mode() {
        Ok(m) => m,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(64);
        }
    };
    let res = match mode {
        Mode::Stage(a) => run(a),
        Mode::Pipeline {
            data,
            output,
            start,
            end,
            workers,
        } => analytics::run_pipeline(&data, &output, start, end, workers),
    };
    if let Err(e) = res {
        eprintln!("{e}");
        std::process::exit(if e.downcast_ref::<InvalidIngest>().is_some() {
            2
        } else {
            1
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::sync::atomic::{AtomicUsize, Ordering};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            let p = std::env::temp_dir().join(format!(
                "shopping-native-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&p).unwrap();
            Self(p)
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    #[test]
    fn typed_empty_all_tables() {
        let temp = Temp::new();
        for table in ["events", "clicks", "products", "campaigns"] {
            let path = temp.0.join(format!("{table}.parquet"));
            assert_eq!(Sink::new(&path, table, 2).unwrap().close().unwrap(), 0);
            let reader =
                ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
            assert_eq!(reader.schema(), &schema(table));
            assert_eq!(reader.metadata().file_metadata().num_rows(), 0);
        }
    }
    #[test]
    fn bounded_rows_and_string_bytes() {
        let temp = Temp::new();
        let path = temp.0.join("products.parquet");
        let mut sink = Sink::new(&path, "products", 2).unwrap();
        for _ in 0..5 {
            sink.push(&[Value::S(Some("p")), Value::S(Some("category"))])
                .unwrap();
        }
        assert_eq!(sink.close().unwrap(), 5);
        let reader = SerializedFileReader::new(File::open(&path).unwrap()).unwrap();
        assert_eq!(reader.num_row_groups(), 3);
        for i in 0..3 {
            assert!(reader.metadata().row_group(i).num_rows() <= 2);
        }
        let path = temp.0.join("large.parquet");
        let mut sink = Sink::new(&path, "products", MAX_ROWS).unwrap();
        let large = "x".repeat(MAX_BYTES / 2);
        sink.push(&[Value::S(Some("p")), Value::S(Some(&large))])
            .unwrap();
        sink.push(&[Value::S(Some("q")), Value::S(Some(&large))])
            .unwrap();
        assert_eq!(sink.rows, 0);
        assert_eq!(sink.close().unwrap(), 2);
    }
    #[test]
    fn physical_header_cr_utf8_errors() {
        let temp = Temp::new();
        let path = temp.0.join("input.csv");
        fs::write(&path, b"header\rignored\n\"a\",b\r\nheader,row\n\n").unwrap();
        let mut found = Vec::new();
        rows(&path, 2, |r| {
            found.push((r[0].unwrap().to_owned(), r[1].unwrap().to_owned()));
            Ok(())
        })
        .unwrap();
        assert_eq!(
            found,
            vec![("a".into(), "b\r".into()), ("header".into(), "row".into())]
        );
        fs::write(&path, b"header\n\xff,b\n").unwrap();
        assert!(rows(&path, 2, |_| Ok(())).is_err());
    }
    fn inputs(temp: &Temp) -> (PathBuf, PathBuf) {
        let data = temp.0.join("data");
        let stage = temp.0.join("stage");
        fs::create_dir(&data).unwrap();
        fs::create_dir(&stage).unwrap();
        for table in ["events", "clicks", "links"] {
            fs::create_dir(data.join(table)).unwrap();
            fs::write(data.join(table).join("header.csv"), "header\n").unwrap();
        }
        fs::write(data.join("products.csv"), "header\np,category\n").unwrap();
        fs::write(data.join("campaigns.csv"), "header\nc,channel\n").unwrap();
        (data, stage)
    }
    #[test]
    fn complete_protocol_and_retained_copies() {
        let temp = Temp::new();
        let (data, stage) = inputs(&temp);
        fs::write(data.join("links/a.csv"), "header\nu,z\nz,A unused\n").unwrap();
        fs::write(data.join("events/a.csv"),"header\ndup,u,bad,2026-10-01T00:00:00Z,purchase,p,1\ndup,z,1790812800000,2026-10-01T00:00:01Z,purchase,p,000250\nlate,u,1790812800000,2026-10-03T00:00:00Z,view,p,garbage\n").unwrap();
        run(Args {
            data,
            stage: stage.clone(),
            start: 1790812800000,
            end: 1792022400000,
            batch: 1,
            workers: 1,
        })
        .unwrap();
        let marker: serde_json::Value =
            serde_json::from_slice(&fs::read(stage.join("_SUCCESS.json")).unwrap()).unwrap();
        assert_eq!(marker["rows"]["events"], 3);
        assert_eq!(marker["rows"]["clicks"], 0);
        let reader = ParquetRecordBatchReaderBuilder::try_new(
            File::open(stage.join("events/part-0.parquet")).unwrap(),
        )
        .unwrap()
        .with_batch_size(1)
        .build()
        .unwrap();
        let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        let first = &batches[0];
        assert_eq!(
            first
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "A unused"
        );
        assert!(
            !first
                .column(7)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );
        assert!(
            batches[1]
                .column(7)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );
        assert!(
            batches[2]
                .column(7)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );
    }
    #[test]
    fn invalid_ingest_has_no_success_marker() {
        let temp = Temp::new();
        let (data, stage) = inputs(&temp);
        fs::write(data.join("events/a.csv"), "header\ne,u,1,bad,view,p,\n").unwrap();
        let error = run(Args {
            data,
            stage: stage.clone(),
            start: 0,
            end: 100,
            batch: 2,
            workers: 1,
        })
        .unwrap_err();
        assert!(error.downcast_ref::<InvalidIngest>().is_some());
        assert!(!stage.join("_SUCCESS.json").exists());
    }
    #[test]
    fn chunk_boundaries_preserve_physical_records() {
        let temp = Temp::new();
        let path = temp.0.join(".csv");
        let content = "header\nα,b\r\nheader,row\nmalformed\n\nlast,unterminated";
        fs::write(&path, content).unwrap();
        let planned = chunks(&[path.clone()], 7).unwrap();
        let mut actual = Vec::new();
        let cancelled = AtomicBool::new(false);
        for chunk in &planned {
            chunk_rows(chunk, 2, &cancelled, |row| {
                actual.push(row.iter().map(|v| v.map(str::to_owned)).collect::<Vec<_>>());
                Ok(())
            })
            .unwrap();
        }
        let mut expected = Vec::new();
        rows(&path, 2, |row| {
            expected.push(row.iter().map(|v| v.map(str::to_owned)).collect::<Vec<_>>());
            Ok(())
        })
        .unwrap();
        assert_eq!(actual, expected);
        assert!(planned.len() > 1);
        for content in ["", "header", "header\n"] {
            fs::write(&path, content).unwrap();
            assert!(chunks(&[path.clone()], 2).unwrap().is_empty());
        }
    }
    #[test]
    fn worker_error_and_panic_cancel_and_join_all() {
        let joined = AtomicUsize::new(0);
        let result = workers(3, |worker, cancelled| -> Result<usize> {
            let _guard = JoinGuard(&joined);
            if worker == 1 {
                return Err(Box::new(InvalidIngest));
            }
            while !cancelled.load(Ordering::Relaxed) {
                std::thread::yield_now();
            }
            Ok(worker)
        });
        assert!(
            result
                .unwrap_err()
                .downcast_ref::<InvalidIngest>()
                .is_some()
        );
        assert_eq!(joined.load(Ordering::Relaxed), 3);
        let result = workers(2, |worker, cancelled| -> Result<usize> {
            if worker == 0 {
                panic!("injected worker panic");
            }
            while !cancelled.load(Ordering::Relaxed) {
                std::thread::yield_now();
            }
            Ok(worker)
        });
        assert!(result.unwrap_err().to_string().contains("panicked"));
    }
    struct JoinGuard<'a>(&'a AtomicUsize);
    impl Drop for JoinGuard<'_> {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    #[test]
    fn parallel_parts_preserve_every_event_with_uneven_chunks() {
        let temp = Temp::new();
        let (data, stage) = inputs(&temp);
        fs::write(
            data.join("events/.csv"),
            "header\nsmall,u,1,2026-10-01T00:00:00Z,view,p,",
        )
        .unwrap();
        fs::write(
            data.join("events/large.csv"),
            format!(
                "header\n{}",
                (0..40)
                    .map(|i| format!(
                        "e{i},u,1,2026-10-01T00:00:00Z,view,{},\n",
                        "é".repeat(i + 1)
                    ))
                    .collect::<String>()
            ),
        )
        .unwrap();
        fs::write(data.join("events/empty.csv"), "").unwrap();
        fs::write(data.join("events/header.csv"), "unterminated header").unwrap();
        let ids = HashMap::from([("u".to_owned(), "canonical".to_owned())]);
        let campaigns = HashSet::new();
        let context = Context {
            ids: &ids,
            campaigns: &campaigns,
            null_campaign: false,
            start: 0,
            end: 100,
        };
        for worker_count in [1, 2, 8] {
            let directory = stage.join(format!("workers-{worker_count}"));
            fs::create_dir_all(directory.join("events")).unwrap();
            let args = Args {
                data: data.clone(),
                stage: directory.clone(),
                start: 0,
                end: 100,
                batch: 2,
                workers: worker_count,
            };
            let parts = stage_table_with_chunk_size(&args, "events", &context, 80).unwrap();
            assert_eq!(parts.len(), worker_count);
            assert_eq!(parts.iter().map(|part| part.rows).sum::<u64>(), 41);
            let mut found = Vec::new();
            for part in parts {
                let builder = ParquetRecordBatchReaderBuilder::try_new(
                    File::open(directory.join("events").join(part.name)).unwrap(),
                )
                .unwrap();
                assert_eq!(builder.schema(), &schema("events"));
                for group in builder.metadata().row_groups() {
                    assert!(group.num_rows() <= 2);
                }
                for batch in builder.build().unwrap() {
                    let batch = batch.unwrap();
                    let event = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let person = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    for i in 0..batch.num_rows() {
                        found.push(event.value(i).to_owned());
                        assert_eq!(person.value(i), "canonical");
                    }
                }
            }
            found.sort();
            let mut expected: Vec<_> = (0..40)
                .map(|i| format!("e{i}"))
                .chain(["small".to_owned()])
                .collect();
            expected.sort();
            assert_eq!(found, expected);
        }
    }
    #[test]
    fn production_chunk_boundary_keeps_very_long_record_once() {
        let temp = Temp::new();
        let path = temp.0.join("large.csv");
        let mut file = File::create(&path).unwrap();
        file.write_all(b"header\n").unwrap();
        file.write_all(&vec![b'x'; MAX_BYTES + 7]).unwrap();
        file.write_all(b",value\nlast,row").unwrap();
        drop(file);
        let planned = chunks(&[path], MAX_BYTES as u64).unwrap();
        assert_eq!(planned.len(), 2);
        let mut count = 0;
        for chunk in planned {
            chunk_rows(&chunk, 2, &AtomicBool::new(false), |_| {
                count += 1;
                Ok(())
            })
            .unwrap();
        }
        assert_eq!(count, 2);
    }
}
