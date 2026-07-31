use std::time::Duration;

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
/// API key is provided (directly, via file, or via the `CIRCULAR_FAST_API_KEY`
/// environment variable).
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
    /// Optional cashback destination pubkey.
    pub cashback_address: Option<String>,
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
            cashback_address: None,
        }
    }
}
