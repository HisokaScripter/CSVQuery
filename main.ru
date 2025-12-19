//! Minimal Rust CLI equivalent of the PowerShell CSV combiner.
//! Features:
//! - Input folder, recursive file discovery with multiple glob patterns (`;` or `,` separated).
//! - Plain filter language: `Column contains foo AND * contains 20,21,22-25` with operators
//!   contains / does not contain / = / != / > / < / >= / <=, AND/OR, `*` column for any column.
//!   Values support comma lists and numeric ranges (e.g., `19-23` expands to 19,20,21,22,23).
//! - Unifies columns across files; missing columns are padded with "".
//! - Streams rows; writes header once and appends rows as they are processed.
//! - Parallel file processing can be added with rayon; this version keeps it simple/streaming
//!   and should already outperform the PowerShell implementation.
//!
//! Build/run (once Rust is installed):
//!   cargo new csvcombiner
//!   cd csvcombiner
//!   # replace src/main.rs with this file’s contents
//!   # add to Cargo.toml dependencies (under [dependencies]):
//!   # csv = "1.3"
//!   # globset = "0.4"
//!   # walkdir = "2"
//!   # clap = { version = "4", features = ["derive"] }
//!   cargo run --release -- --input C:\path\to\folder --output C:\out.csv --patterns "*.csv;*MFT*.csv" --filter "* contains 2025 AND * contains 11 AND * contains 19-23"

use clap::Parser;
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use globset::{Glob, GlobSet, GlobSetBuilder};
use rayon::ThreadPoolBuilder;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use walkdir::WalkDir;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Op {
    Contains,
    NotContains,
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
}

#[derive(Debug, Clone)]
struct Condition {
    column: String,
    op: Op,
    values: Vec<String>, // expanded list (ranges expanded)
}

#[derive(Debug, Clone)]
struct Filter {
    conditions: Vec<Condition>,
    connectors: Vec<String>, // AND / OR, length = conditions.len() - 1
}

#[derive(Parser, Debug)]
#[command(name = "CSV Combiner", version)]
struct Cli {
    /// Input folder (recursive search)
    #[arg(short, long)]
    input: PathBuf,

    /// Output file (CSV). If a directory is given, a timestamped CSV is created.
    #[arg(short, long)]
    output: PathBuf,

    /// File patterns separated by ';' or ',' (e.g., "*.csv;*MFT*.csv")
    #[arg(short, long, default_value = "*.csv")]
    patterns: String,

    /// Filter text, e.g., `Status contains Open AND * contains 20,21,22-25`
    #[arg(short, long, default_value = "")]
    filter: String,
}

fn build_globset(patterns: &str) -> anyhow::Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pat in patterns
        .split(|c| c == ';' || c == ',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        builder.add(Glob::new(pat)?);
    }
    Ok(builder.build()?)
}

fn find_files(root: &Path, gs: &GlobSet) -> Vec<PathBuf> {
    WalkDir::new(root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter(|e| gs.is_match(e.path()))
        .map(|e| e.into_path())
        .collect()
}

fn expand_values(raw: &str) -> Vec<String> {
    let mut out = Vec::new();
    for part in raw
        .split(',')
        .map(|s| s.trim().trim_matches('"').trim_matches('\''))
        .filter(|s| !s.is_empty())
    {
        if let Some((a, b)) = part.split_once('-') {
            if let (Ok(start), Ok(end)) = (a.trim().parse::<i64>(), b.trim().parse::<i64>()) {
                let (lo, hi) = if start <= end { (start, end) } else { (end, start) };
                for v in lo..=hi {
                    out.push(v.to_string());
                }
                continue;
            }
        }
        out.push(part.to_string());
    }
    if out.is_empty() {
        out.push(raw.to_string());
    }
    out
}

fn parse_op(op: &str) -> Option<Op> {
    match op.to_lowercase().as_str() {
        "contains" => Some(Op::Contains),
        "does not contain" | "not contains" | "not contain" | "!contains" => Some(Op::NotContains),
        "=" | "==" => Some(Op::Eq),
        "!=" => Some(Op::Ne),
        ">" => Some(Op::Gt),
        "<" => Some(Op::Lt),
        ">=" => Some(Op::Ge),
        "<=" => Some(Op::Le),
        _ => None,
    }
}

fn parse_condition(text: &str) -> anyhow::Result<Condition> {
    // pattern: <col> <op> <val> ; col may be quoted or [bracketed]
    let re = regex::Regex::new(
        r#"^\s*(?:"(?P<col>[^"]+)"|\[(?P<bcol>[^\]]+)\]|(?P<ccol>[^\s<>!=]+))\s+(?P<op>>=|<=|>|<|!=|==|=|does\s+not\s+contain|not\s+contains|not\s+contain|!contains|contains)\s+(?P<val>.+?)\s*$"#,
    )?;
    let caps = re
        .captures(text)
        .ok_or_else(|| anyhow::anyhow!("Could not parse condition: '{}'", text))?;
    let col = caps
        .name("col")
        .or_else(|| caps.name("bcol"))
        .or_else(|| caps.name("ccol"))
        .map(|m| m.as_str().trim().to_string())
        .ok_or_else(|| anyhow::anyhow!("Missing column in condition: '{}'", text))?;
    let op_str = caps
        .name("op")
        .ok_or_else(|| anyhow::anyhow!("Missing operator in condition: '{}'", text))?
        .as_str();
    let op = parse_op(op_str).ok_or_else(|| anyhow::anyhow!("Unsupported operator '{}'", op_str))?;
    let val = caps
        .name("val")
        .ok_or_else(|| anyhow::anyhow!("Missing value in condition: '{}'", text))?
        .as_str();
    let values = expand_values(val);
    Ok(Condition { column: col, op, values })
}

fn parse_filter(text: &str) -> anyhow::Result<Option<Filter>> {
    if text.trim().is_empty() {
        return Ok(None);
    }
    let normalized = text.replace('(', " ").replace(')', " ");
    let splitter = regex::Regex::new(r"(?i)\s+(and|or)\s+")?;

    let mut conditions = Vec::new();
    let mut connectors = Vec::new();
    let mut last = 0usize;
    let bytes = normalized.as_bytes();
    for mat in splitter.find_iter(&normalized) {
        let start = mat.start();
        let end = mat.end();
        if start > last {
            let cond_str = std::str::from_utf8(&bytes[last..start])?.trim();
            if !cond_str.is_empty() {
                conditions.push(parse_condition(cond_str)?);
            }
        }
        connectors.push(mat.as_str().trim().to_uppercase());
        last = end;
    }
    // trailing piece
    if last < normalized.len() {
        let cond_str = normalized[last..].trim();
        if !cond_str.is_empty() {
            conditions.push(parse_condition(cond_str)?);
        }
    }

    if conditions.is_empty() {
        return Ok(None);
    }
    if conditions.len() > 1 && connectors.len() != conditions.len() - 1 {
        return Err(anyhow::anyhow!("Malformed filter: check AND/OR spacing"));
    }
    Ok(Some(Filter { conditions, connectors }))
}

fn value_matches(op: &Op, record_val: &str, cond_vals: &[String]) -> bool {
    let to_num = |s: &str| s.parse::<f64>().ok();
    match op {
        Op::Contains => cond_vals.iter().any(|v| record_val.contains(v)),
        Op::NotContains => cond_vals.iter().all(|v| !record_val.contains(v)),
        Op::Eq => cond_vals.iter().any(|v| record_val == v),
        Op::Ne => cond_vals.iter().all(|v| record_val != v),
        Op::Gt => cond_vals.iter().any(|v| match (to_num(record_val), to_num(v)) {
            (Some(a), Some(b)) => a > b,
            _ => record_val > v.as_str(),
        }),
        Op::Lt => cond_vals.iter().any(|v| match (to_num(record_val), to_num(v)) {
            (Some(a), Some(b)) => a < b,
            _ => record_val < v.as_str(),
        }),
        Op::Ge => cond_vals.iter().any(|v| match (to_num(record_val), to_num(v)) {
            (Some(a), Some(b)) => a >= b,
            _ => record_val >= v.as_str(),
        }),
        Op::Le => cond_vals.iter().any(|v| match (to_num(record_val), to_num(v)) {
            (Some(a), Some(b)) => a <= b,
            _ => record_val <= v.as_str(),
        }),
    }
}

fn record_matches(filter: &Filter, headers: &HashMap<String, usize>, record: &StringRecord) -> bool {
    let eval_condition = |cond: &Condition| -> bool {
        if cond.column == "*" {
            // any column
            let mut matched_any = matches!(cond.op, Op::NotContains | Op::Ne | Op::Le | Op::Lt);
            for val in record.iter() {
                let s = val;
                match cond.op {
                    Op::NotContains => {
                        if cond.values.iter().any(|v| s.contains(v)) {
                            matched_any = false;
                            break;
                        } else {
                            matched_any = true;
                        }
                    }
                    Op::Ne => {
                        if cond.values.iter().any(|v| s == v) {
                            matched_any = false;
                            break;
                        } else {
                            matched_any = true;
                        }
                    }
                    Op::Contains | Op::Eq | Op::Gt | Op::Lt | Op::Ge | Op::Le => {
                        if value_matches(&cond.op, s, &cond.values) {
                            matched_any = true;
                            break;
                        }
                    }
                }
            }
            matched_any
        } else if let Some(idx) = headers.get(&cond.column) {
            let val = record.get(*idx).unwrap_or_default();
            value_matches(&cond.op, val, &cond.values)
        } else {
            // missing column => treat as empty string
            value_matches(&cond.op, "", &cond.values)
        }
    };

    let mut result = eval_condition(&filter.conditions[0]);
    for (i, conn) in filter.connectors.iter().enumerate() {
        let next = eval_condition(&filter.conditions[i + 1]);
        if conn == "AND" {
            result = result && next;
        } else {
            result = result || next;
        }
    }
    result
}

fn collect_headers(files: &[PathBuf]) -> anyhow::Result<Vec<String>> {
    let mut set: HashSet<String> = HashSet::new();
    for path in files {
        let mut rdr = ReaderBuilder::new().has_headers(true).from_path(path)?;
        if let Some(headers) = rdr.headers().ok() {
            for h in headers.iter() {
                let h = h.to_string();
                if !h.is_empty() {
                    set.insert(h);
                }
            }
        }
    }
    let mut cols: Vec<String> = set.into_iter().collect();
    cols.sort();
    Ok(cols)
}

fn ensure_output_path(output: &Path) -> anyhow::Result<PathBuf> {
    if output.extension().and_then(|s| s.to_str()) == Some("csv") {
        if let Some(parent) = output.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        Ok(output.to_path_buf())
    } else {
        // treat as directory
        std::fs::create_dir_all(output)?;
        let ts = chrono::Local::now().format("output_%Y%m%d_%H%M%S.csv");
        Ok(output.join(ts.to_string()))
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let globset = build_globset(&cli.patterns)?;
    let files = find_files(&cli.input, &globset);
    if files.is_empty() {
        anyhow::bail!("No CSV files found under {:?} for patterns {:?}", cli.input, cli.patterns);
    }

    let filter = parse_filter(&cli.filter)?;
    let headers = collect_headers(&files)?;
    if headers.is_empty() {
        anyhow::bail!("No headers detected.");
    }

    let out_path = ensure_output_path(&cli.output)?;
    let need_header = !out_path.exists();
    let out_file = File::options()
        .create(true)
        .append(true)
        .write(true)
        .open(&out_path)?;
    let mut writer = WriterBuilder::new()
        .has_headers(need_header)
        .from_writer(BufWriter::new(out_file));
    if need_header {
        writer.write_record(&headers)?;
    }

    let (tx, rx) = mpsc::channel::<Vec<Vec<String>>>();
    let writer_handle = thread::spawn(move || -> anyhow::Result<usize> {
        let mut count = 0usize;
        for batch in rx {
            for row in batch {
                writer.write_record(&row)?;
                count += 1;
            }
        }
        writer.flush()?;
        Ok(count)
    });

    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let max_threads = cores.saturating_sub(4).max(1).min(files.len().max(1));

    ThreadPoolBuilder::new()
        .num_threads(max_threads)
        .build()?
        .scope(|s| {
            for path in files {
                let tx = tx.clone();
                let headers = headers.clone();
                let filter = filter.clone();
                s.spawn(move |_| {
                    let mut batch = Vec::new();
                    if let Ok(mut rdr) = ReaderBuilder::new().has_headers(true).from_path(&path) {
                        let header_map: HashMap<String, usize> = match rdr.headers() {
                            Ok(h) => h
                                .iter()
                                .enumerate()
                                .map(|(i, h)| (h.to_string(), i))
                                .collect(),
                            Err(_) => HashMap::new(),
                        };
                        for rec in rdr.records().flatten() {
                            let passes = filter
                                .as_ref()
                                .map(|f| record_matches(f, &header_map, &rec))
                                .unwrap_or(true);
                            if !passes {
                                continue;
                            }
                            let mut out_row = Vec::with_capacity(headers.len());
                            for h in &headers {
                                if let Some(idx) = header_map.get(h) {
                                    out_row.push(rec.get(*idx).unwrap_or_default().to_string());
                                } else {
                                    out_row.push(String::new());
                                }
                            }
                            batch.push(out_row);
                        }
                    }
                    let _ = tx.send(batch);
                });
            }
        });
    drop(tx);

    let total_rows = writer_handle.join().unwrap_or(Ok(0))?;
    eprintln!(
        "Done. Rows written: {} -> {} (threads used: {})",
        total_rows,
        out_path.display(),
        max_threads
    );
    Ok(())
}
