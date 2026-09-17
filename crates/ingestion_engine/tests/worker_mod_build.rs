use ingestion_engine::application::worker::{WorkerState, Worker};
use pkg::proto::coordinator::ExampleRequest;
use pkg::proto::worker::ReadMapRequest;

#[test]
fn test_worker_module_importable() {
    let state = WorkerState::new();
    assert!(state.data.is_empty());
    let state2 = WorkerState { data: std::collections::HashMap::new() };
    assert!(state2.data.is_empty());
}

#[test]
fn test_worker_state_data_visibility() {
    let mut state = WorkerState::new();
    state.data.insert(1, std::collections::HashMap::new());
    assert!(state.data.contains_key(&1));
}

#[test]
fn test_pkg_proto_via_worker() {
    let _ = pkg::proto::coordinator::SubmitJobRequest {
        files: vec!["a.txt".into()],
        output_dir: "out".into(),
        app: "test".into(),
        n_reduce: 1,
        args: vec![],
    };
    let _ = ExampleRequest { name: "x".into() };
    let _ = ReadMapRequest { job_id: 1, map_task: 0, reduce_task: 0 };
}
