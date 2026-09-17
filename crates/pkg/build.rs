fn main() {

    let _ = std::fs::create_dir("src/proto");
    let _ = tonic_prost_build::configure()
        .out_dir("src/proto")
        .compile_protos(
            &["proto/ingestion_engine/coordinator.proto", "proto/ingestion_engine/worker.proto"],
            &["proto/"],
        );
}
