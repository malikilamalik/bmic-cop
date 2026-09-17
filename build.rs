use std::io::Result;
use std::fs;
use std::path::Path;

fn main() -> Result<()> {
    let _ = std::fs::create_dir("crates/pkg/src/proto");
    
    let ingestion_engine_proto_dir = "proto/ingestion_engine";
    // let core_module_proto_dir = "proto/ingestion_engine"; 

    let mut proto_files = Vec::new();
    for entry in fs::read_dir(ingestion_engine_proto_dir)? {
        let entry = entry?;
        let path = entry.path();
        
        // Only include files that have a .proto extension
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("proto") {
            proto_files.push(path);
        }
    }

    prost_build::compile_protos(&proto_files, &["/crates/pkg/src/proto"])?;
    Ok(())
}