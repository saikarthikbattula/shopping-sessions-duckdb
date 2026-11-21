mod identity;
mod parsing;
use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    sync::Arc,
};
type Result<T> = std::result::Result<T, Box<dyn Error>>;
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
}
fn args() -> std::result::Result<Args, String> {
    let mut values = HashMap::new();
    let mut it = std::env::args().skip(1);
    while let Some(key) = it.next() {
        if ![
            "--data-dir",
            "--stage-dir",
            "--window-start-ms",
            "--window-end-ms",
            "--batch-rows",
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
    Ok(Args {
        data: PathBuf::from(get("--data-dir")?),
        stage: PathBuf::from(get("--stage-dir")?),
        start,
        end,
        batch: batch.min(MAX_ROWS as u64) as usize,
    })
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
    let mut event = Sink::new(&a.stage.join("events/part-0.parquet"), "events", a.batch)?;
    for file in parts(&a.data.join("events"))? {
        rows(&file, 7, |r| {
            if r[0].is_none() {
                return Ok(());
            }
            let ts = parsing::window(r[2], i128::from(a.start), i128::from(a.end));
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
            event.push(&[
                Value::S(r[0]),
                Value::S(person(&ids, r[1])),
                Value::I(Some(ingest)),
                Value::I(ts),
                Value::S(r[4]),
                Value::S(r[5]),
                Value::I(cents),
                Value::B(valid),
            ])
        })?;
    }
    let events = event.close()?;
    let mut click = Sink::new(&a.stage.join("clicks/part-0.parquet"), "clicks", a.batch)?;
    for file in parts(&a.data.join("clicks"))? {
        rows(&file, 4, |r| {
            if r[0].is_none() {
                return Ok(());
            }
            let ts = parsing::window(r[3], i128::from(a.start) - 604800000, i128::from(a.end));
            let valid = r[1].is_some()
                && ts.is_some()
                && r[2].map_or(null_campaign, |id| campaigns.contains(id));
            click.push(&[
                Value::S(r[0]),
                Value::S(person(&ids, r[1])),
                Value::S(r[2]),
                Value::I(ts),
                Value::B(valid),
            ])
        })?;
    }
    let clicks = click.close()?;
    let marker = serde_json::json!({"protocol_version":1,"window_start_ms":a.start,"window_end_ms":a.end,"rows":{"events":events,"clicks":clicks,"products":products,"campaigns":campaign_count}});
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
    let a = match args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(64)
        }
    };
    if let Err(e) = run(a) {
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
        })
        .unwrap_err();
        assert!(error.downcast_ref::<InvalidIngest>().is_some());
        assert!(!stage.join("_SUCCESS.json").exists());
    }
}
