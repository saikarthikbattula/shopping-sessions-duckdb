mod analytics;
mod csv_batch;
#[cfg(test)]
mod parsing;

use std::{collections::HashMap, error::Error, fs, path::PathBuf};
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug)]
struct InvalidIngest;
impl std::fmt::Display for InvalidIngest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ingest_ts violates the guaranteed valid timestamp input")
    }
}
impl Error for InvalidIngest {}

#[derive(Debug)]
struct Args {
    data: PathBuf,
    output: PathBuf,
    start: Option<i64>,
    end: Option<i64>,
    workers: usize,
}

fn quota_cpus(text: &str) -> Option<usize> {
    let mut fields = text.split_whitespace();
    let quota = fields.next()?.parse::<u64>().ok()?;
    let period = fields.next()?.parse::<u64>().ok()?;
    if quota == 0 || period == 0 || fields.next().is_some() { return None; }
    usize::try_from(quota.div_ceil(period)).ok()
}

fn detect_cpus() -> usize {
    let v2 = fs::read_to_string("/sys/fs/cgroup/cpu.max").ok()
        .and_then(|text| quota_cpus(&text));
    let v1 = || {
        let quota = fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us").ok()?;
        let period = fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us").ok()?;
        quota_cpus(&format!("{} {}", quota.trim(), period.trim()))
    };
    v2.or_else(v1).unwrap_or_else(|| std::thread::available_parallelism()
        .map(usize::from).unwrap_or(8)).clamp(1, 16)
}

fn parse_args(raw: &[String], mut args: Args) -> std::result::Result<Args, String> {
    if raw.len() == 2 && raw.iter().all(|v| !v.starts_with('-')) {
        args.data = PathBuf::from(&raw[0]);
        args.output = PathBuf::from(&raw[1]);
        return Ok(args);
    }
    let mut values = HashMap::new();
    let mut options = raw.iter();
    while let Some(key) = options.next() {
        if !["--data-dir", "--output-dir", "--window-start-ms", "--window-end-ms", "--workers"]
            .contains(&key.as_str()) { return Err(format!("unknown option {key}")); }
        let value = options.next().ok_or_else(|| format!("missing value for {key}"))?;
        if values.insert(key.as_str(), value.as_str()).is_some() { return Err(format!("duplicate option {key}")); }
    }
    if let Some(value) = values.get("--data-dir") { args.data = PathBuf::from(value); }
    if let Some(value) = values.get("--output-dir") { args.output = PathBuf::from(value); }
    if let Some(value) = values.get("--workers") {
        args.workers = value.parse().map_err(|_| "invalid workers count")?;
        if args.workers == 0 { return Err("invalid workers count".into()); }
    }
    let timestamp = |key| values.get(key).map(|v| v.parse::<i64>()
        .map_err(|_| format!("invalid {key}"))).transpose();
    args.start = timestamp("--window-start-ms")?;
    args.end = timestamp("--window-end-ms")?;
    match (args.start, args.end) {
        (Some(start), Some(end)) if start < end => (),
        (None, None) => (),
        _ => return Err("supply both window boundaries with start < end".into()),
    }
    Ok(args)
}

fn cli() -> std::result::Result<Args, String> {
    let workers = match std::env::var("NCPU") {
        Ok(value) => value.parse::<usize>().ok().filter(|v| *v > 0)
            .ok_or("NCPU must be a positive integer")?,
        Err(_) => detect_cpus(),
    };
    let defaults = Args {
        data: std::env::var_os("DATA_DIR").map(PathBuf::from).unwrap_or_else(|| "/data".into()),
        output: std::env::var_os("OUTPUT_DIR").map(PathBuf::from).unwrap_or_else(|| "/output".into()),
        start: None, end: None, workers,
    };
    parse_args(&std::env::args().skip(1).collect::<Vec<_>>(), defaults)
}

fn main() {
    let args = cli().unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(64);
    });
    if let Err(error) = analytics::run_pipeline(&args.data, &args.output, args.start, args.end, args.workers) {
        eprintln!("{error}");
        std::process::exit(if error.downcast_ref::<InvalidIngest>().is_some() { 2 } else { 1 });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn defaults() -> Args { Args { data: "/data".into(), output: "/output".into(), start: None, end: None, workers: 8 } }
    fn parse(raw: &[&str]) -> std::result::Result<Args, String> {
        parse_args(&raw.iter().map(|s| s.to_string()).collect::<Vec<_>>(), defaults())
    }
    #[test]
    fn cgroup_quota_rounds_up_without_overflow() {
        assert_eq!(quota_cpus("150000 100000\n"), Some(2));
        assert_eq!(quota_cpus("18446744073709551615 18446744073709551615"), Some(1));
        for input in ["max 100000", "-1 100000", "0 100000", "100 0", "", "100", "1 2 3"] {
            assert_eq!(quota_cpus(input), None);
        }
    }
    #[test]
    fn cli_default_positional_and_explicit_paths() {
        assert_eq!(parse(&[]).unwrap().data, PathBuf::from("/data"));
        assert_eq!(parse(&["data", "out"]).unwrap().output, PathBuf::from("out"));
        let args = parse(&["--data-dir", "in", "--output-dir", "out", "--workers", "2",
            "--window-start-ms", "0", "--window-end-ms", "10"]).unwrap();
        assert_eq!((args.workers,args.start,args.end), (2,Some(0),Some(10)));
    }
    #[test]
    fn cli_rejects_retired_staging_flags_and_bad_options() {
        for raw in [vec!["--stage-dir","out"], vec!["--workers","0"], vec!["--workers","bad"],
            vec!["--data-dir"], vec!["--workers","1","--workers","2"],
            vec!["--window-start-ms","0"], vec!["--window-start-ms","bad","--window-end-ms","1"],
            vec!["--window-start-ms","2","--window-end-ms","1"]] {
            assert!(parse(&raw).is_err(), "{raw:?}");
        }
    }
}
