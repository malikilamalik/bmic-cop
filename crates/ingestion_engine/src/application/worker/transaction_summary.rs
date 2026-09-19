use super::{KeyValue, MapOutput, parse_transaction_line};
use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};


/// Encoding: 8 bytes sum (amount), 8 bytes count(1), 8 bytes debit(0/1), 8 bytes credit(0/1)
pub fn map(kv: KeyValue) -> MapOutput {
    let s = String::from_utf8(kv.value.to_vec())?;
    let mut out: Vec<anyhow::Result<KeyValue>> = Vec::new();
    for line in s.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(p) = parse_transaction_line(line) {
            let customer_id = if !p.ref_id.is_empty() { p.ref_id } else { p.id.clone() };
            let amount: u64 = p.amount.parse::<u64>().unwrap_or(0);
            let is_debit = p.tx_type.to_lowercase() == "debit";
            let is_credit = p.tx_type.to_lowercase() == "credit";
            let mut vb = BytesMut::with_capacity(32);
            vb.put_u64(amount);
            vb.put_u64(1);
            vb.put_u64(if is_debit { 1 } else { 0 });
            vb.put_u64(if is_credit { 1 } else { 0 });
            out.push(Ok(KeyValue::new(
                Bytes::from(customer_id.into_bytes()),
                vb.freeze(),
            )));
        } else {
            // malformed line -> count as unknown with 0 amount
            let mut vb = BytesMut::with_capacity(32);
            vb.put_u64(0);
            vb.put_u64(1);
            vb.put_u64(0);
            vb.put_u64(0);
            out.push(Ok(KeyValue::new(
                Bytes::from(b"unknown".to_vec()),
                vb.freeze(),
            )));
        }
    }
    Ok(Box::new(out.into_iter()))
}

/// Reduce: per customer_id, sum encoded values
/// Returns encoded 32 bytes: sum, count, count_debit, count_credit
pub fn reduce(_key: Bytes, values: Box<dyn Iterator<Item = Bytes> + '_>) -> Result<Bytes> {
    let mut sum: u64 = 0;
    let mut count: u64 = 0;
    let mut debit: u64 = 0;
    let mut credit: u64 = 0;
    for mut v in values {
        if v.len() >= 32 {
            sum += v.get_u64();
            count += v.get_u64();
            debit += v.get_u64();
            credit += v.get_u64();
        } else {
            // fallback: value may be raw line (should not happen with new map)
            // try to parse as transaction line if possible - handled in map, so fallback counts as 1
            count += 1;
        }
    }
    let mut out = BytesMut::with_capacity(32);
    out.put_u64(sum);
    out.put_u64(count);
    out.put_u64(debit);
    out.put_u64(credit);
    Ok(out.freeze())
}

/// ProcessOutput: decode aggregated per-customer and format txt
/// Output: customer_id,sum,count,avg(2 decimals),count_debit,count_credit sorted by customer_id
pub fn process_output(kva: Box<dyn Iterator<Item = KeyValue>>) -> Result<String> {
    use std::cmp::Ordering;
    use std::fmt::Write;

    let mut decoded: Vec<(String, u64, u64, f64, u64, u64)> = Vec::new();
    for kv in kva {
        let k = String::from_utf8(kv.key.to_vec())?;
        let mut v = kv.value;
        if v.len() < 32 {
            continue;
        }
        let sum = v.get_u64();
        let count = v.get_u64();
        let debit = v.get_u64();
        let credit = v.get_u64();
        let avg = if count > 0 { sum as f64 / count as f64 } else { 0.0 };
        decoded.push((k, sum, count, avg, debit, credit));
    }
    // sort by customer_id ascending (if equal, not needed)
    decoded.sort_by(|a, b| {
        let ord = a.0.cmp(&b.0);
        if ord == Ordering::Equal {
            a.1.cmp(&b.1)
        } else {
            ord
        }
    });
    let mut s = String::new();
    for (k, sum, count, avg, debit, credit) in decoded {
        writeln!(&mut s, "{},{},{},{:.2},{},{}", k, sum, count, avg, debit, credit)?;
    }
    Ok(s)
}
