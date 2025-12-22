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
enum Operator {
    Contains,
    NotContains,
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterEqual,
    LessEqual,
}

#[derive(Debug, Clone)]
struct Condition {
    column: String,
    operator: Operator,
    values: Vec<String>,
}

#[derive(Debug, Clone)]
struct Filter {
    conditions: Vec<Condition>,
    connectors: Vec<String>,
}

#[derive(Parser, Debug)]
#[command(name = "CSV Combiner", version)]
struct CliArgs {
    #[arg(short, long)]
    input: PathBuf,

    #[arg(short, long)]
    output: PathBuf,

    #[arg(short, long, default_value = "*.csv")]
    patterns: String,

    #[arg(short, long, default_value = "")]
    filter: String,
}

fn build_globset(patterns: &str) -> anyhow::Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns
        .split(|c| c == ';' || c == ',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        builder.add(Glob::new(pattern)?);
    }
    Ok(builder.build()?)
}

fn discover_files(root: &Path, globset: &GlobSet) -> Vec<PathBuf> {
    WalkDir::new(root)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .filter(|e| globset.is_match(e.path()))
        .map(|e| e.into_path())
        .collect()
}

fn expand_values(raw: &str) -> Vec<String> {
    let mut expanded = Vec::new();

    for token in raw
        .split(',')
        .map(|s| s.trim().trim_matches('"').trim_matches('\''))
        .filter(|s| !s.is_empty())
    {
        if let Some((start, end)) = token.split_once('-') {
            if let (Ok(a), Ok(b)) = (start.trim().parse::<i64>(), end.trim().parse::<i64>()) {
                let (low, high) = if a <= b { (a, b) } else { (b, a) };
                for n in low..=high {
                    expanded.push(n.to_string());
                }
                continue;
            }
        }
        expanded.push(token.to_string());
    }

    if expanded.is_empty() {
        expanded.push(raw.to_string());
    }

    expanded
}

fn parse_operator(input: &str) -> Option<Operator> {
    match input.to_lowercase().as_str() {
        "contains" => Some(Operator::Contains),
        "does not contain" | "not contains" | "not contain" | "!contains" => {
            Some(Operator::NotContains)
        }
        "=" | "==" => Some(Operator::Equal),
        "!=" => Some(Operator::NotEqual),
        ">" => Some(Operator::GreaterThan),
        "<" => Some(Operator::LessThan),
        ">=" => Some(Operator::GreaterEqual),
        "<=" => Some(Operator::LessEqual),
        _ => None,
    }
}

fn parse_condition(text: &str) -> anyhow::Result<Condition> {
    let regex = regex::Regex::new(
        r#"^\s*(?:"(?P<col>[^"]+)"|\[(?P<bcol>[^\]]+)\]|(?P<ucol>[^\s<>!=]+))\s+(?P<op>>=|<=|>|<|!=|==|=|does\s+not\s+contain|not\s+contains|not\s+contain|!contains|contains)\s+(?P<val>.+?)\s*$"#,
    )?;

    let caps = regex
        .captures(text)
        .ok_or_else(|| anyhow::anyhow!("Invalid condition: '{}'", text))?;

    let column = caps
        .name("col")
        .or_else(|| caps.name("bcol"))
        .or_else(|| caps.name("ucol"))
        .map(|m| m.as_str().trim().to_string())
        .ok_or_else(|| anyhow::anyhow!("Missing column in '{}'", text))?;

    let operator_str = caps
        .name("op")
        .ok_or_else(|| anyhow::anyhow!("Missing operator in '{}'", text))?
        .as_str();

    let operator =
        parse_operator(operator_str).ok_or_else(|| anyhow::anyhow!("Bad operator"))?;

    let value_str = caps
        .name("val")
        .ok_or_else(|| anyhow::anyhow!("Missing value in '{}'", text))?
        .as_str();

    Ok(Condition {
        column,
        operator,
        values: expand_values(value_str),
    })
}

fn parse_filter(text: &str) -> anyhow::Result<Option<Filter>> {
    if text.trim().is_empty() {
        return Ok(None);
    }

    let normalized = text.replace('(', " ").replace(')', " ");
    let splitter = regex::Regex::new(r"(?i)\s+(and|or)\s+")?;

    let mut conditions = Vec::new();
    let mut connectors = Vec::new();
    let mut last_index = 0;
    let bytes = normalized.as_bytes();

    for m in splitter.find_iter(&normalized) {
        let start = m.start();
        let end = m.end();

        if start > last_index {
            let slice = std::str::from_utf8(&bytes[last_index..start])?.trim();
            if !slice.is_empty() {
                conditions.push(parse_condition(slice)?);
            }
        }

        connectors.push(m.as_str().trim().to_uppercase());
        last_index = end;
    }

    if last_index < normalized.len() {
        let tail = normalized[last_index..].trim();
        if !tail.is_empty() {
            conditions.push(parse_condition(tail)?);
        }
    }

    if conditions.len() > 1 && connectors.len() != conditions.len() - 1 {
        anyhow::bail!("Malformed filter expression");
    }

    Ok(Some(Filter {
        conditions,
        connectors,
    }))
}

fn compare_value(operator: &Operator, record_value: &str, values: &[String]) -> bool {
    let to_number = |s: &str| s.parse::<f64>().ok();

    match operator {
        Operator::Contains => values.iter().any(|v| record_value.contains(v)),
        Operator::NotContains => values.iter().all(|v| !record_value.contains(v)),
        Operator::Equal => values.iter().any(|v| record_value == v),
        Operator::NotEqual => values.iter().all(|v| record_value != v),
        Operator::GreaterThan => values.iter().any(|v| match (to_number(record_value), to_number(v))
        {
            (Some(a), Some(b)) => a > b,
            _ => record_value > v,
        }),
        Operator::LessThan => values.iter().any(|v| match (to_number(record_value), to_number(v)) {
            (Some(a), Some(b)) => a < b,
            _ => record_value < v,
        }),
        Operator::GreaterEqual => {
            values.iter().any(|v| match (to_number(record_value), to_number(v)) {
                (Some(a), Some(b)) => a >= b,
                _ => record_value >= v,
            })
        }
        Operator::LessEqual => values.iter().any(|v| match (to_number(record_value), to_number(v))
        {
            (Some(a), Some(b)) => a <= b,
            _ => record_value <= v,
        }),
    }
}

fn record_matches_filter(
    filter: &Filter,
    header_index: &HashMap<String, usize>,
    record: &StringRecord,
    pdir: Option<&str>,
) -> bool {
    let evaluate = |condition: &Condition| -> bool {
        if condition.column == "*" {
            let mut matched = matches!(
                condition.operator,
                Operator::NotContains | Operator::NotEqual | Operator::LessThan | Operator::LessEqual
            );

            for field in record.iter() {
                if compare_value(&condition.operator, field, &condition.values) {
                    matched = true;
                    break;
                }
            }
            matched
        } else if condition.column == "PDIR" {
            compare_value(
                &condition.operator,
                pdir.unwrap_or_default(),
                &condition.values,
            )
        } else if let Some(index) = header_index.get(&condition.column) {
            compare_value(
                &condition.operator,
                record.get(*index).unwrap_or_default(),
                &condition.values,
            )
        } else {
            compare_value(&condition.operator, "", &condition.values)
        }
    };

    let mut result = evaluate(&filter.conditions[0]);

    for (i, connector) in filter.connectors.iter().enumerate() {
        let next = evaluate(&filter.conditions[i + 1]);
        result = if connector == "AND" {
            result && next
        } else {
            result || next
        };
    }

    result
}

fn collect_all_headers(files: &[PathBuf]) -> anyhow::Result<Vec<String>> {
    let mut headers = HashSet::new();

    for file in files {
        let mut reader = ReaderBuilder::new().has_headers(true).from_path(file)?;
        if let Ok(hdrs) = reader.headers() {
            for h in hdrs {
                if !h.is_empty() {
                    headers.insert(h.to_string());
                }
            }
        }
    }

    let mut result: Vec<String> = headers.into_iter().collect();
    result.sort();

    if !result.iter().any(|h| h == "PDIR") {
        result.push("PDIR".to_string());
    }

    Ok(result)
}

fn resolve_output_path(path: &Path) -> anyhow::Result<PathBuf> {
    if path.extension().and_then(|s| s.to_str()) == Some("csv") {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(path.to_path_buf())
    } else {
        std::fs::create_dir_all(path)?;
        let filename = chrono::Local::now().format("output_%Y%m%d_%H%M%S.csv");
        Ok(path.join(filename.to_string()))
    }
}

fn build_pdir_value(file: &Path, root: &Path) -> String {
    let relative = file.strip_prefix(root).unwrap_or(file);
    relative
        .components()
        .map(|c| c.as_os_str().to_string_lossy().to_string())
        .collect::<Vec<_>>()
        .join("/")
}

fn main() -> anyhow::Result<()> {
    let args = CliArgs::parse();

    let globset = build_globset(&args.patterns)?;
    let files = discover_files(&args.input, &globset);

    if files.is_empty() {
        anyhow::bail!("No CSV files found");
    }

    let filter = parse_filter(&args.filter)?;
    let headers = collect_all_headers(&files)?;
    let output_path = resolve_output_path(&args.output)?;

    let needs_header = !output_path.exists();
    let file = File::options().create(true).append(true).open(&output_path)?;
    let mut writer = WriterBuilder::new()
        .has_headers(needs_header)
        .from_writer(BufWriter::new(file));

    if needs_header {
        writer.write_record(&headers)?;
    }

    let (tx, rx) = mpsc::channel::<Vec<Vec<String>>>();

    let writer_thread = thread::spawn(move || -> anyhow::Result<usize> {
        let mut count = 0;
        for batch in rx {
            for row in batch {
                writer.write_record(&row)?;
                count += 1;
            }
        }
        writer.flush()?;
        Ok(count)
    });

    let available_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .saturating_sub(4)
        .max(1);

    ThreadPoolBuilder::new()
        .num_threads(available_threads)
        .build()?
        .scope(|scope| {
            for path in files {
                let tx = tx.clone();
                let headers = headers.clone();
                let filter = filter.clone();
                let root = args.input.clone();

                scope.spawn(move |_| {
                    let mut rows = Vec::new();

                    if let Ok(mut reader) =
                        ReaderBuilder::new().has_headers(true).from_path(&path)
                    {
                        let header_map = reader
                            .headers()
                            .map(|h| {
                                h.iter()
                                    .enumerate()
                                    .map(|(i, name)| (name.to_string(), i))
                                    .collect::<HashMap<_, _>>()
                            })
                            .unwrap_or_default();

                        let pdir = build_pdir_value(&path, &root);

                        for record in reader.records().flatten() {
                            let allowed = filter
                                .as_ref()
                                .map(|f| record_matches_filter(f, &header_map, &record, Some(&pdir)))
                                .unwrap_or(true);

                            if !allowed {
                                continue;
                            }

                            let row = headers
                                .iter()
                                .map(|h| {
                                    if h == "PDIR" {
                                        pdir.clone()
                                    } else if let Some(i) = header_map.get(h) {
                                        record.get(*i).unwrap_or_default().to_string()
                                    } else {
                                        String::new()
                                    }
                                })
                                .collect();

                            rows.push(row);
                        }
                    }

                    let _ = tx.send(rows);
                });
            }
        });

    drop(tx);

    let written = writer_thread.join().unwrap_or(Ok(0))?;
    eprintln!(
        "Done. Rows written: {} → {}",
        written,
        output_path.display()
    );

    Ok(())
}
