use pkg::proto::coordinator::ExampleRequest;
use pkg::proto::worker::ReadMapRequest;
use pkg::log;

#[test]
fn test_pkg_proto_coordinator_importable() {
    let req = ExampleRequest { name: "test".into() };
    assert_eq!(req.name, "test");
}

#[test]
fn test_pkg_proto_worker_importable() {
    let req = ReadMapRequest { job_id: 1, map_task: 2, reduce_task: 3 };
    assert_eq!(req.job_id, 1);
}

#[test]
fn test_pkg_log_importable() {
    let _ = log::info as fn(&str);
}
