fn main() -> Result<(), std::io::Error> {
    // Prefer an explicit PROTOC, otherwise rely on `protoc` on PATH.
    // Avoids compiling `protobuf-src` (fragile on some darwin toolchains).
    let proto_base_path = std::path::PathBuf::from("proto");
    let proto = proto_base_path.join("fast_tx.proto");
    println!("cargo:rerun-if-changed={}", proto.display());

    // The server side is generated for the test receiver binary and the
    // integration tests; the validator itself only uses the client.
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&[proto], &[proto_base_path])
}
