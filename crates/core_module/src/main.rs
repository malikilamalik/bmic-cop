use pkg::log;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use zen_engine::DecisionEngine;
use zen_engine::model::DecisionContent;

// Keep import for scheduler if needed elsewhere (unused now)
#[allow(unused_imports)]
use core_module::application::scheduler::Scheduler;

#[derive(Debug, Clone)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl KeyValue {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self { key, value }
    }
}

fn decode_output_file(data: &[u8]) -> Vec<KeyValue> {
    let mut out = Vec::new();
    let mut off = 0usize;
    while off + 4 <= data.len() {
        let len = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
        off += 4;
        if off + len > data.len() {
            break;
        }
        let key = data[off..off + len].to_vec();
        off += len;

        if off + 4 > data.len() {
            break;
        }
        let len2 = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
        off += 4;
        if off + len2 > data.len() {
            break;
        }
        let value = data[off..off + len2].to_vec();
        off += len2;

        out.push(KeyValue::new(key, value));
    }
    out
}

async fn process_output(
    kva: Box<dyn Iterator<Item = KeyValue>>,
    decision: &zen_engine::Decision,
) -> anyhow::Result<String> {
    use std::fmt::Write;

    let mut out = String::new();
    for kv in kva {
        let cid = String::from_utf8_lossy(&kv.key).to_string();
        if kv.value.len() < 32 {
            // skip malformed, mirrors benefit_evaluator::process_output check
            log::warn!(
                "skipping key={} with value len {} < 32",
                cid,
                kv.value.len()
            );
            continue;
        }
        let sum = u64::from_be_bytes(kv.value[0..8].try_into().unwrap());
        let count = u64::from_be_bytes(kv.value[8..16].try_into().unwrap());
        let debit = u64::from_be_bytes(kv.value[16..24].try_into().unwrap());
        let credit = u64::from_be_bytes(kv.value[24..32].try_into().unwrap());
        let avg = if count > 0 {
            sum as f64 / count as f64
        } else {
            0.0
        };

        // The JSON input the user specified: sum/count under transaction.transaction_today
        let input = json!({
            "transaction": {
                "transaction_today": {
                    "sum": sum,
                    "count": count
                }
            }
        });

        // Zen evaluation: `let result = decision.evaluate(json!({...}).into()).await;`
        let resp = decision.evaluate(input.clone().into()).await;
        let (is_match, benefit_type, benefit_amount) = match resp {
            Ok(r) => {
                // `r.result` is Variable; serialize to Value to inspect `benefit.*`
                let v: Value = serde_json::to_value(&r.result).unwrap_or(Value::Null);
                // cashback.json outputs `benefit.type` and `benefit.amount`
                let b_type = v
                    .get("benefit")
                    .and_then(|b| b.get("type"))
                    .and_then(|t| t.as_str())
                    .unwrap_or("");
                let b_amount = v
                    .get("benefit")
                    .and_then(|b| b.get("amount"))
                    .and_then(|a| a.as_u64())
                    .unwrap_or(0);
                let matched = b_type == "CASHBACK" && b_amount != 0;
                (matched, b_type.to_string(), b_amount)
            }
            Err(e) => {
                log::warn!("zen evaluate failed for user {}: {:?}", cid, e);
                (false, "".to_string(), 0)
            }
        };

        let status = if is_match { "MATCH" } else { "NO_MATCH" };
        // Log per user as requested
        log::info!(
            "user {} sum={} count={} avg={:.2} debit={} credit={} => {} benefit={} amount={}",
            cid,
            sum,
            count,
            avg,
            debit,
            credit,
            status,
            benefit_type,
            benefit_amount
        );
        println!(
            "user={} sum={} count={} avg={:.2} debit={} credit={} => {} benefit_type={} benefit_amount={}",
            cid, sum, count, avg, debit, credit, status, benefit_type, benefit_amount
        );

        // Build output string line, mirroring benefit_evaluator sorted output plus match flag
        if is_match {
            writeln!(
                &mut out,
                "{},{},{},{:.2},{},{},{},{}:{}",
                cid, sum, count, avg, debit, credit, status, benefit_type, benefit_amount
            )?;
        } else {
            writeln!(
                &mut out,
                "{},{},{},{:.2},{},{},{}",
                cid, sum, count, avg, debit, credit, status
            )?;
        }
    }
    Ok(out)
}

fn find_rule_file() -> Option<PathBuf> {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let candidates = [
        PathBuf::from("data/rule/cashback.json"),
        PathBuf::from("./data/rule/cashback.json"),
        PathBuf::from(format!("{}/../../data/rule/cashback.json", manifest_dir)),
        PathBuf::from(format!("{}/../../../data/rule/cashback.json", manifest_dir)),
        PathBuf::from(format!("{}/data/rule/cashback.json", manifest_dir)),
        PathBuf::from("/tmp/data/rule/cashback.json"),
    ];
    for p in candidates {
        if p.exists() {
            return Some(p);
        }
    }
    None
}

fn find_output_dir() -> Option<PathBuf> {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let candidates = [
        PathBuf::from("data/output"),
        PathBuf::from("./data/output"),
        PathBuf::from(format!("{}/../../data/output", manifest_dir)),
        PathBuf::from(format!("{}/../../../data/output", manifest_dir)),
        PathBuf::from(format!("{}/data/output", manifest_dir)),
        PathBuf::from("/output"),
        PathBuf::from("/tmp/output"),
    ];
    for p in candidates {
        if p.exists() && p.is_dir() {
            return Some(p);
        }
    }
    None
}

fn find_benefit_dir(base_output_dir: &Path) -> PathBuf {
    // Always under data/output/benefit; if base is e.g. data/output, join benefit
    // If base is something else like /output, still join benefit
    base_output_dir.join("benefit")
}

fn resolve_benefit_dir_fallback() -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let candidates = [
        PathBuf::from("data/output/benefit"),
        PathBuf::from("./data/output/benefit"),
        PathBuf::from(format!("{}/../../data/output/benefit", manifest_dir)),
        PathBuf::from(format!("{}/../../../data/output/benefit", manifest_dir)),
        PathBuf::from(format!("{}/data/output/benefit", manifest_dir)),
    ];
    for p in &candidates {
        if let Some(parent) = p.parent() {
            if parent.exists() {
                return p.clone();
            }
        }
    }
    // default to first candidate
    candidates[0].clone()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    log::init_logger();

    // Load DecisionContent from data/rule/cashback.json
    let rule_path_opt = find_rule_file();
    let decision_name = rule_path_opt
        .as_ref()
        .and_then(|p| {
            p.file_stem()
                .and_then(|s| s.to_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "cashback".to_string());

    let decision_content: DecisionContent = if let Some(rule_path) = rule_path_opt.as_ref() {
        log::info!("loading rule from {:?}", rule_path);
        let s = tokio::fs::read_to_string(&rule_path).await?;
        serde_json::from_str(&s)?
    } else {
        // fallback to compile-time include (original path corrected to ../../../data/rule/cashback.json)
        log::info!("rule file not found via runtime search, using include_str fallback");
        serde_json::from_str(include_str!("../../../data/rule/cashback.json")).unwrap()
    };

    let engine = DecisionEngine::default();
    let decision = engine.create_decision(Arc::new(decision_content)).unwrap();

    // Read data/output/* as requested
    let output_dir = find_output_dir();
    let output_dir = match output_dir {
        Some(d) => d,
        None => {
            log::warn!("data/output directory not found; tried candidates. No files to evaluate.");
            println!(
                "No output directory found (searched data/output, ./data/output, manifest relatives, /output). Put mr-out-* files in data/output/"
            );
            return Ok(());
        }
    };

    log::info!("reading output files from {:?}", output_dir);

    let mut entries = tokio::fs::read_dir(&output_dir).await?;
    let mut files: Vec<PathBuf> = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let p = entry.path();
        if p.is_file() {
            if let Some(fname) = p.file_name().and_then(|s| s.to_str()) {
                if fname.starts_with("mr-out-") {
                    files.push(p);
                }
            }
        }
    }
    files.sort();

    // Also support glob data/output/* if no mr-out- prefix but user said data/output/*
    if files.is_empty() {
        let mut entries = tokio::fs::read_dir(&output_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let p = entry.path();
            if p.is_file() {
                // skip benefit subdir if accidentally listed (shouldn't happen as we only read output_dir)
                if p.extension().and_then(|s| s.to_str()) == Some("txt")
                    && p.parent()
                        .map(|par| par.ends_with("benefit"))
                        .unwrap_or(false)
                {
                    continue;
                }
                files.push(p);
            }
        }
        files.sort();
    }

    if files.is_empty() {
        log::warn!("no files found in {:?}", output_dir);
        println!("No files in {:?}", output_dir);
        return Ok(());
    }

    let mut all_kvs: Vec<KeyValue> = Vec::new();
    for f in &files {
        // skip benefit directory files if output_dir contains it (shouldn't, but guard)
        if f.to_string_lossy().contains("/benefit/") {
            continue;
        }
        let data = tokio::fs::read(f).await?;
        if data.is_empty() {
            log::info!("file {:?} empty, skipping", f);
            continue;
        }
        let kvs = decode_output_file(&data);
        log::info!("decoded {} KVs from {:?}", kvs.len(), f);
        all_kvs.extend(kvs);
    }

    // Sort by customer_id to mirror benefit_evaluator::process_output sorting
    all_kvs.sort_by(|a, b| a.key.cmp(&b.key));

    // Evaluate via Zen: process_output does per-user match printing
    let output = process_output(Box::new(all_kvs.into_iter()), &decision).await?;
    println!("--- process_output result ---\n{}", output);
    log::info!("process_output done, {} bytes", output.len());

    // Write output into files to data/output/benefit/{datetime}-{decisionname}.txt
    let datetime = chrono::Utc::now().format("%Y%m%d-%H%M%S").to_string();
    // Alternative format without dash: "%Y%m%d%H%M%S" also common; we use hyphen as it matches {datetime}-{decisionname} pattern
    let benefit_dir = find_benefit_dir(&output_dir);
    let benefit_dir = if benefit_dir.exists() {
        benefit_dir
    } else {
        // create directory (and fallback handling)
        match tokio::fs::create_dir_all(&benefit_dir).await {
            Ok(_) => benefit_dir,
            Err(e) => {
                log::warn!(
                    "failed to create benefit dir {:?}: {:?}, trying fallback",
                    benefit_dir,
                    e
                );
                let fallback = resolve_benefit_dir_fallback();
                tokio::fs::create_dir_all(&fallback).await?;
                fallback
            }
        }
    };
    // Ensure dir exists
    tokio::fs::create_dir_all(&benefit_dir).await?;

    let out_path = benefit_dir.join(format!("{}-{}.txt", datetime, decision_name));
    tokio::fs::write(&out_path, output.as_bytes()).await?;
    log::info!("wrote benefit output to {:?}", out_path);
    println!("wrote benefit output to {:?}", out_path);

    Ok(())
}