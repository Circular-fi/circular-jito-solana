use std::{path::PathBuf, time::Duration};

/// Production Circular Fast gRPC endpoint (fast-rust, gRPC on :8081; note
/// :8080 is the HTTP JSON-RPC and :50051 is Jito shredstream). Operators only
/// need to provide an API key; the URL defaults here so the exporter is plug
/// & play.
pub const DEFAULT_URL: &str = "http://cashback.circular.fi";
pub const DEFAULT_QUEUE_CAPACITY: usize = 8_192;
pub const DEFAULT_MAX_IN_FLIGHT: usize = 1_024;
pub const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 5_000;
pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 2_000;

/// Configuration of the Circular Fast exporter. Built from the
/// `--circular-fast-*` CLI flags; the exporter is entirely disabled unless an
/// API key is provided (via file or the `CIRCULAR_FAST_API_KEY` environment
/// variable).
#[derive(Clone, Debug)]
pub struct CircularExportConfig {
    /// Fast gRPC endpoint, e.g. `http://cashback.circular.fi`.
    pub url: String,
    /// API key sent as the `x-api-key` gRPC metadata on every request. Same
    /// key as the Fast HTTP API.
    pub api_key: String,
    /// Capacity, in packet batches, of the queue between sigverify and the
    /// exporter thread. When full, new batches are dropped.
    pub queue_capacity: usize,
    /// Maximum number of concurrent in-flight `SendTransaction` calls. When
    /// reached, further transactions are dropped rather than queued — this is
    /// the backpressure valve that keeps a slow/dead Fast endpoint from ever
    /// stalling the validator.
    pub max_in_flight: usize,
    /// Timeout of a single gRPC connection attempt.
    pub connect_timeout: Duration,
    /// Timeout of a single `SendTransaction` call. Bounds how long an
    /// in-flight slot can be held.
    pub request_timeout: Duration,
    /// Optional memo attached to every submission (tracing/context).
    pub memo: Option<String>,
    /// Optional cashback destination pubkey.
    pub cashback_address: Option<String>,
    /// Custom root CA certificate (PEM) for TLS endpoints.
    pub tls_ca_path: Option<PathBuf>,
    /// Export TPU vote transactions as well. Off by default — votes are
    /// irrelevant to the ARB stream.
    pub include_votes: bool,
}

impl Default for CircularExportConfig {
    fn default() -> Self {
        Self {
            url: DEFAULT_URL.to_string(),
            api_key: String::new(),
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            connect_timeout: Duration::from_millis(DEFAULT_CONNECT_TIMEOUT_MS),
            request_timeout: Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS),
            memo: None,
            cashback_address: None,
            tls_ca_path: None,
            include_votes: false,
        }
    }
}
