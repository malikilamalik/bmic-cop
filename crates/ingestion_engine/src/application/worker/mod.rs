use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use itertools::Itertools;
use pkg::log;
use pkg::{
    config::{coordinator::CoordinatorConfig, worker::WorkerConfig},
    proto::{coordinator::*, worker::*},
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::time::Duration;
use tonic::transport::{Channel, Server};
use tonic::Code;
use tonic::Request;
use tonic::Response;
use tonic::Status;
pub use transaction_summary::map as transaction_summary_map;
pub use transaction_summary::reduce as transaction_summary_reduce;


// --- MapReduce shims (transaction mapping) ---
type TaskNumber = u32;

#[derive(Debug, Clone)]
pub struct KeyValue {
    pub key: Bytes,
    pub value: Bytes,
}

impl KeyValue {
    /// Construct a new key-value pair from the given key and value.
    pub fn new(key: Bytes, value: Bytes) -> Self {
        Self { key, value }
    }

    /// Get the key of this key-value pair.
    ///
    /// This method is cheap, since [`Bytes`] are cheaply cloneable.
    #[inline]
    pub fn key(&self) -> Bytes {
        self.key.clone()
    }

    /// Get the value of this key-value pair.
    ///
    /// This method is cheap, since [`Bytes`] are cheaply cloneable.
    #[inline]
    pub fn value(&self) -> Bytes {
        self.value.clone()
    }

    /// Consumes the key-value pair and returns the key.
    #[inline]
    pub fn into_key(self) -> Bytes {
        self.key
    }

    /// Consumes the key-value pair and returns the value.
    #[inline]
    pub fn into_value(self) -> Bytes {
        self.value
    }
}

pub type MapOutput = anyhow::Result<Box<dyn Iterator<Item = anyhow::Result<KeyValue>>>>;
pub type MapFn = fn(KeyValue) -> MapOutput;
pub type ReduceFn = fn(
    key: Bytes,
    values: Box<dyn Iterator<Item = Bytes> + '_>,
) -> anyhow::Result<Bytes>;

pub type ProcessOutputFn = fn(kvs: Box<dyn Iterator<Item = KeyValue>>) -> anyhow::Result<String>;


fn ihash(key: &Bytes) -> u32 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    h.finish() as u32
}

mod codec {
    use bytes::{Buf, BytesMut};

use super::Bytes;
    pub struct LengthDelimitedReader {
        buf: Bytes,
    }
    impl LengthDelimitedReader {
        /// Creates a new reader that reads from the given buffer.
        pub fn new(buf: Bytes) -> Self {
            Self { buf }
        }
    }

    pub struct LengthDelimitedWriter {
        pub buf: BytesMut,
    }
    impl LengthDelimitedWriter {
        pub fn new() -> Self {
            Self { buf: BytesMut::new() }
        }
        pub fn send(&mut self, data: Bytes) {
            let len = data.len() as u32;
            self.buf.extend_from_slice(&len.to_le_bytes());
            self.buf.extend_from_slice(&data);
        }
        pub fn finish(self) -> BytesMut {
            self.buf
        }
        pub fn len(&self) -> usize {
            self.buf.len()
        }
    }
    impl Iterator for LengthDelimitedReader {
        type Item = Bytes;
        fn next(&mut self) -> Option<Self::Item> {
            if !self.buf.has_remaining() {
                return None;
            }

            let len = self.buf.get_u32_le();
            let chunk = self.buf.split_to(len as usize);
            Some(chunk)
        }
    }
}
use codec::{LengthDelimitedReader, LengthDelimitedWriter};

pub mod transaction_summary;

pub struct App {
    pub map_fn: MapFn,
    pub reduce_fn: ReduceFn,
    pub process_output_fn: ProcessOutputFn,
}

pub fn get_app(name: &str) -> Option<App> {
    match name {
        "benefit_evaluator" | "benefit-evaluator" => Some(App {
            map_fn: transaction_summary::map,
            reduce_fn: transaction_summary::reduce,
            process_output_fn: transaction_summary::process_output,
        }),
        _ => None,
    }
}



fn dummy_reduce(_key: Bytes, values: Box<dyn Iterator<Item = Bytes> + '_>) -> Result<Bytes> {
    let mut out = Vec::new();
    for v in values {
        if !out.is_empty() {
            out.extend_from_slice(b",");
        }
        out.extend_from_slice(&v);
    }
    Ok(Bytes::from(out))
}

/// Transaction-aware map function: splits file content into lines,
/// parses via `parse_transaction_line`, and emits key=customer_id (ref_id), value=original line.
/// This enables reduce to aggregate per customer: sum,count,avg,debit/credit counts.
pub fn transaction_map_fn(kv: KeyValue) -> MapOutput {
    let content = String::from_utf8_lossy(&kv.value).to_string();
    let mut out: Vec<anyhow::Result<KeyValue>> = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(p) = parse_transaction_line(line) {
            let customer_id = if !p.ref_id.is_empty() { p.ref_id } else { p.id };
            out.push(Ok(KeyValue::new(
                Bytes::from(customer_id.into_bytes()),
                Bytes::from(line.as_bytes().to_vec()),
            )));
        } else {
            out.push(Ok(KeyValue::new(
                Bytes::from(b"unknown".to_vec()),
                Bytes::from(line.as_bytes().to_vec()),
            )));
        }
    }
    Ok(Box::new(out.into_iter()))
}

pub struct WorkerState {
    data: HashMap<u32, HashMap<(u32, u32), Bytes>>,
}

impl WorkerState {
    fn get_data(&self, job_id: u32, map_task: u32, reduce_task: u32) -> Option<Bytes> {
        let task_map = self.data.get(&job_id)?;
        let bytes = task_map.get(&(map_task, reduce_task))?;
        Some(bytes.clone())
    }
}

#[tonic::async_trait]
impl worker_server::Worker for Worker {
    async fn read_map(
        &self,
        req: Request<ReadMapRequest>,
    ) -> Result<Response<ReadMapReply>, Status> {
        let req = req.into_inner();
        let state = self.inner.lock().await;
        let data = state.get_data(
            req.job_id,
            req.map_task,
            req.reduce_task,
        );
        if data.is_none() {
            return Err(Status::new(
                Code::InvalidArgument,
                "worker does not have data for the given task",
            ));
        }
        let data = data.unwrap();

        Ok(Response::new(ReadMapReply {
            result: data.as_ref().into(),
        }))
    }

    async fn remove_job(
        &self,
        req: Request<RemoveJobRequest>,
    ) -> Result<Response<RemoveJobReply>, Status> {
        let req = req.into_inner();
        let mut state = self.inner.lock().await;
        state.data.remove(&req.job_id);
        Ok(Response::new(RemoveJobReply {}))
    }
}

#[derive(Clone)]
pub struct Worker {
    id: u32,
    client: coordinator_client::CoordinatorClient<Channel>,
    inner: Arc<Mutex<WorkerState>>,
    worker_config: WorkerConfig,
    coor_config: CoordinatorConfig
}

impl Worker {
    pub async fn new(
        coor_config: &CoordinatorConfig,
        worker_config: &WorkerConfig,
    ) -> Result<Self> {
        let coor_config = coor_config.clone();
        let worker_config = worker_config.clone();
        let mut client = coordinator_client::CoordinatorClient::connect(format!(
            "http://{}",
            coor_config.addr()
        ))
        .await?;
        let res = client.register(RegisterRequest {}).await?;
        Ok(Self {
            id: res.get_ref().worker_id,
            client,
            inner: Arc::new(Mutex::new(WorkerState {
                data: HashMap::new(),
            })),
            worker_config: worker_config,
            coor_config: coor_config
        })
    }

    async fn handle_task(&mut self, reply: GetTaskReply) -> Result<()> {
        if reply.wait {
            log::info!(
                "Worker {} waiting {} ms",
                self.id,
                self.worker_config.wait_time_ms
            );
            tokio::time::sleep(Duration::from_millis(self.worker_config.wait_time_ms)).await;
            return Ok(());
        }

        log::info!(
            "Worker {} processing task job_id={} file={} output_dir={} reduce={} n_reduce={} app={} key={} entity={}",
            self.id, reply.job_id, reply.file, reply.output_dir, reply.reduce, reply.n_reduce, reply.app, reply.key, reply.entity
        );

        let n_reduce = if reply.n_reduce == 0 {
            4
        } else {
            reply.n_reduce
        };
        let app = if reply.app.is_empty() {
            "benefit-evaluator".to_string()
        } else {
            reply.app.clone()
        };

        if reply.reduce {
            // Reduce phase: aggregate intermediate data and write to output_dir
            // Dispatch via App for benefit_evaluator: map/reduce/process_output
            let aux = Bytes::new();
            let app_entry = get_app(&app);
            let reduce_fn: ReduceFn = app_entry.as_ref().map(|a| a.reduce_fn).unwrap_or(dummy_reduce);
            let process_output_fn: ProcessOutputFn = app_entry
                .as_ref()
                .map(|a| a.process_output_fn)
                .unwrap_or(|kva: Box<dyn Iterator<Item = KeyValue>>| {
                    let mut s = String::new();
                    for kv in kva {
                        s.push_str(&String::from_utf8_lossy(&kv.value));
                        s.push('\n');
                    }
                    Ok(s)
                });
            match self
                .reduce(
                    reduce_fn,
                    reply.map_task_assignments,
                    reply.job_id,
                    reply.task,
                    &reply.output_dir,
                )
                .await
            {
                Ok(None) => log::info!("Worker {} reduce task {} succeeded", self.id, reply.task),
                Ok(Some(msg)) => log::warn!(
                    "Worker {} reduce task {} returned message: {}",
                    self.id,
                    reply.task,
                    msg
                ),
                Err(e) => log::error!(
                    "Worker {} reduce task {} failed: {:?}",
                    self.id,
                    reply.task,
                    e
                ),
            }
        } else {
            // Map phase: read, log, and map file transaction
            match Self::read_and_log_file(&reply.file).await {
                Ok(cnt) => log::info!(
                    "Worker {} read {} transaction lines from {}",
                    self.id,
                    cnt,
                    reply.file
                ),
                Err(e) => log::warn!(
                    "Worker {} failed to read file {}: {:?}",
                    self.id,
                    reply.file,
                    e
                ),
            }

            log::info!(
                "Worker {} running map for app={} n_reduce={}",
                self.id,
                app,
                n_reduce
            );
            let aux = Bytes::new();
            let map_fn = get_app(&app).map(|a| a.map_fn).unwrap_or(transaction_map_fn);
            match self
                .map(
                    map_fn,
                    reply.job_id,
                    reply.task,
                    n_reduce,
                    reply.file.clone(),
                    aux,
                )
                .await
            {
                Ok(None) => log::info!("Worker {} map task {} succeeded", self.id, reply.task),
                Ok(Some(msg)) => log::warn!(
                    "Worker {} map task {} returned message: {}",
                    self.id,
                    reply.task,
                    msg
                ),
                Err(e) => log::error!("Worker {} map task {} failed: {:?}", self.id, reply.task, e),
            }
        }

        println!(
            "id {} task {} reduce {} worker {}",
            reply.job_id, reply.task, reply.reduce, self.id
        );
        self.client
            .finish_task(FinishTaskRequest {
                worker_id: self.id,
                job_id: reply.job_id,
                task: reply.task,
                reduce: reply.reduce,
            })
            .await?;
        Ok(())
    }

    async fn read_and_log_file(path: &str) -> Result<usize> {
        // Try the path as-is, then fall back to dynamic resolutions
        // so bare filenames like "transaction20260915.txt" resolve to data/transaction/...
        let content = match tokio::fs::read_to_string(path).await {
            Ok(c) => c,
            Err(_) => {
                let basename = std::path::Path::new(path)
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or(path);
                let workspace_root =
                    env!("CARGO_MANIFEST_DIR").trim_end_matches("/crates/ingestion_engine");
                let candidates = [
                    format!("data/transaction/{}", basename),
                    format!("data/transaction/{}", path),
                    format!("./data/transaction/{}", basename),
                    format!("./data/transaction/{}", path),
                    format!("{}/data/transaction/{}", workspace_root, basename),
                    format!("{}/data/transaction/{}", workspace_root, path),
                    format!("{}/{}", workspace_root, path),
                    format!("{}/{}", workspace_root, basename),
                ];
                let mut last_err = None;
                let mut found = None;
                for cand in &candidates {
                    match tokio::fs::read_to_string(cand).await {
                        Ok(c) => {
                            found = Some(c);
                            break;
                        }
                        Err(e) => last_err = Some(e),
                    }
                }
                if let Some(c) = found {
                    log::info!(
                        "Resolved file '{}' via fallback (basename '{}')",
                        path,
                        basename
                    );
                    c
                } else {
                    return Err(anyhow::anyhow!(
                        "cannot read file '{}' (basename '{}'): {:?}",
                        path,
                        basename,
                        last_err
                    ));
                }
            }
        };

        if content.trim().is_empty() {
            log::info!("File {} is empty", path);
            return Ok(0);
        }

        let mut count = 0;
        for (idx, raw_line) in content.lines().enumerate() {
            let line = raw_line.trim();
            if line.is_empty() {
                continue;
            }
            count += 1;
            // raw output
            log::info!("[{}] raw: {}", idx, line);

            if let Some(p) = parse_transaction_line(line) {
                log::info!(
                    "parsed line {} -> id={}, ref_id={}, timestamp={}, amount={}, type={}, desc={}",
                    idx,
                    p.id,
                    p.ref_id,
                    p.timestamp,
                    p.amount,
                    p.tx_type,
                    p.description
                );
            } else {
                log::warn!(
                    "line {}: unexpected format (expected 6 columns): {}",
                    idx,
                    line
                );
            }
        }
        Ok(count)
    }

    async fn read_map_from(&self, worker: u32, req: ReadMapRequest) -> Result<ReadMapReply> {
        let mut map_worker = connect(worker, self.worker_config.initial_worker_port.clone()).await?;
        let res = map_worker.read_map(req).await?.into_inner();
        Ok(res)
    }

    pub async fn reduce(
        &mut self,
        reduce_fn: ReduceFn,
        map_tasks: Vec<MapTaskAssignment>,
        job_id: u32,
        task: u32,
        output_dir: &str,
    ) -> Result<Option<String>> {
        log::info!(
            "Worker {} received reduce task {} for job id {}",
            self.id,
            task,
            job_id
        );

        let mut intermediate = Vec::new();

        for map_task in map_tasks.into_iter() {
            let res = match self
                .read_map_from(
                    map_task.worker_id,
                    ReadMapRequest {
                        job_id,
                        map_task: map_task.task,
                        reduce_task: task,
                    },
                )
                .await
            {
                Ok(res) => res,
                Err(e) => return Ok(Some(e.to_string())),
            };

            log::info!(
                "Worker {} reducing map task {} for job id: {}",
                self.id,
                map_task.task,
                job_id
            );

            let buf = Bytes::from(res.result);
            let mut reader = LengthDelimitedReader::new(buf);

            while let Some(key) = reader.next() {
                let value = match reader.next() {
                    Some(x) => x,
                    None => bail!("Found key without a value"),
                };

                intermediate.push(KeyValue { key, value });
            }
        }

        intermediate.sort_unstable_by_key(KeyValue::key);

        let mut writer = LengthDelimitedWriter::new();

        for (key, group) in &intermediate.into_iter().chunk_by(KeyValue::key) {
            let iter = group.map(KeyValue::into_value);
            let output = reduce_fn(key.clone(), Box::new(iter))?;

            writer.send(key);
            writer.send(output);
        }

        let buf = writer.finish().freeze();
        let name = format!("{}/mr-out-{}", output_dir, task);
        let mut out_file = tokio::fs::File::create(name).await?;
        out_file.write_all(&buf).await?;
        Ok(None)
    }

    pub async fn run(mut self) -> Result<()> {
        let mut client_clone = self.client.clone();
        let wid = self.id;
        let interval = self.worker_config.heartbeat_interval_ms;
        tokio::spawn(async move {
            loop {
                log::info!("Worker {} sending hearbeat", wid);
                let _ = client_clone
                    .heartbeat(HeartbeatRequest { worker_id: wid })
                    .await;
                tokio::time::sleep(Duration::from_millis(interval)).await;
            }
        });

        loop {
            log::info!("Worker {} requesting task", self.id);
            let res = self
                .client
                .get_task(GetTaskRequest { worker_id: self.id })
                .await?;

            if let Err(e) = self.handle_task(res.into_inner()).await {
                log::info!(
                    "Worker {} failed to complete task, waiting {} ms: {:?}",
                    self.id,
                    self.worker_config.wait_time_ms,
                    e,
                );
                tokio::time::sleep(Duration::from_millis(self.worker_config.wait_time_ms)).await;
            }
        }
    }

    pub async fn map(
        &mut self,
        map_fn: MapFn,
        job_id: u32,
        task: u32,
        n_reduce: u32,
        file: String,
        aux: Bytes,
    ) -> Result<Option<String>> {
        let mut state = self.inner.lock().await;

        log::info!("Worker {} received map task {}", self.id, task);

        let mut writers = Vec::with_capacity(n_reduce as usize);
        for _ in 0..n_reduce {
            let writer = LengthDelimitedWriter::new();
            writers.push(writer);
        }

        // Read file content with dynamic fallback (handles bare filenames like "transaction20260915.txt")
        let file_bytes: Vec<u8> = match tokio::fs::read(&file).await {
            Ok(b) => b,
            Err(_) => {
                let basename = std::path::Path::new(&file)
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or(&file);
                let workspace_root =
                    env!("CARGO_MANIFEST_DIR").trim_end_matches("/crates/ingestion_engine");
                let candidates = [
                    format!("data/transaction/{}", basename),
                    format!("data/transaction/{}", file),
                    format!("./data/transaction/{}", basename),
                    format!("./data/transaction/{}", file),
                    format!("{}/data/transaction/{}", workspace_root, basename),
                    format!("{}/data/transaction/{}", workspace_root, file),
                    format!("{}/{}", workspace_root, file),
                    format!("{}/{}", workspace_root, basename),
                ];
                let mut found: Option<Vec<u8>> = None;
                for cand in &candidates {
                    if let Ok(b) = tokio::fs::read(cand).await {
                        log::info!(
                            "Resolved map file '{}' via fallback '{}' (basename '{}')",
                            file,
                            cand,
                            basename
                        );
                        found = Some(b);
                        break;
                    }
                }
                if let Some(b) = found {
                    b
                } else {
                    let mut f = tokio::fs::File::open(&file)
                        .await
                        .with_context(|| format!("Failed to open input file {}", &file))?;
                    let mut buf = Vec::new();
                    f.read_to_end(&mut buf).await?;
                    buf
                }
            }
        };

        let content = Bytes::from(file_bytes);

        let kv = KeyValue {
            key: Bytes::from(file.clone()),
            value: content,
        };

        for item in map_fn(kv)? {
            let KeyValue { key, value } = item?;
            let reduce_idx = ihash(&key) % n_reduce;

            let writer = writers.get_mut(reduce_idx as usize).unwrap();
            writer.send(key);
            writer.send(value);
        }

        let job_entry = state.data.entry(job_id).or_insert_with(HashMap::new);

        for (i, writer) in writers.into_iter().enumerate() {
            job_entry.insert(
                (task as u32, i as u32),
                Bytes::from(writer.finish()),
            );
        }

        log::info!(
            "Worker {} map task {} produced {} partitions for job {}",
            self.id,
            task,
            n_reduce,
            job_id
        );
        Ok(None)
    }
}

/// Parsed representation of a transaction .txt line.
#[derive(Debug, PartialEq)]
pub struct ParsedTransaction {
    pub id: String,
    pub ref_id: String,
    pub timestamp: String,
    pub amount: String,
    pub tx_type: String,
    pub description: String,
}

/// Parse a single .txt line like:
/// `1,1,2026-09-14T03:21:15Z,15000050,Debit,Pembayaran merchant menggunakan kartu debit`
pub fn parse_transaction_line(line: &str) -> Option<ParsedTransaction> {
    let line = line.trim();
    if line.is_empty() {
        return None;
    }
    let parts: Vec<&str> = line.splitn(6, ',').collect();
    if parts.len() < 6 {
        return None;
    }
    Some(ParsedTransaction {
        id: parts[0].trim().to_string(),
        ref_id: parts[1].trim().to_string(),
        timestamp: parts[2].trim().to_string(),
        amount: parts[3].trim().to_string(),
        tx_type: parts[4].trim().to_string(),
        description: parts[5].trim().to_string(),
    })
}

fn get_port(worker_id: u32, initial_worker_port: u16) -> u16 {
    let port = initial_worker_port as u32 + worker_id;
    assert!(port <= u16::MAX as u32);
    port as u16
}

fn get_addr(worker_id: u32, initial_worker_port: u16) -> SocketAddr {
    format!("127.0.0.1:{}", get_port(worker_id, initial_worker_port))
        .parse()
        .unwrap()
}

async fn worker_server(worker: Worker) -> Result<()> {
    let addr = get_addr(worker.id, worker.worker_config.initial_worker_port);
    let svc = worker_server::WorkerServer::new(worker);
    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}

pub async fn start(coor_config: &CoordinatorConfig, worker_config: &WorkerConfig) -> Result<()> {
    let worker = Worker::new(&coor_config, worker_config).await?;

    let server = worker.clone();
    tokio::spawn(async move { worker_server(server).await });
    worker.run().await?;

    Ok(())
}

async fn connect(id: u32, initial_worker_port: u16) -> Result<worker_client::WorkerClient<Channel>> {
    let client =
        worker_client::WorkerClient::connect(format!("http://127.0.0.1:{}", get_port(id, initial_worker_port))).await?;
    Ok(client)
}