# Grafana — Circular Fast + BAM

Dashboard JSON: [`circular-bam-exporter.json`](circular-bam-exporter.json)

Tracks export of BAM-verified transactions to **Circular Fast**, plus BAM path baselines (`receive-and-buffer` / connection).

For pipeline architecture, see [`CIRCULAR.md`](../CIRCULAR.md).

## Import

1. Start Influx + Grafana (`metrics/scripts/start.sh`) and `source metrics/scripts/enable.sh` before launching the validator.
2. Grafana → **Import** → paste `circular-bam-exporter.json` (or upload the file).
3. Datasource: **`local-influxdb`** (database `testnet`).

Circular panels stay empty until:

- the exporter is enabled (API key), and
- BAM batches are being processed (leader + traffic).

The **BAM connection** panel can already move out of leader (heartbeats).

## Panels & fields

### Throughput (recv / enqueue / sent)

Measurement: `circular_transaction_exporter`

| Grafana field | Influx metric | Meaning | How to read |
|---------------|---------------|---------|-------------|
| `received` | `received_transactions` | Txs seen at exporter submit time (after dequeue / filter) | Candidate volume for Fast |
| `enqueued` | `enqueued_transactions` | Txs accepted into the bounded queue (`try_send` OK) | Should track non-vote BAM traffic |
| `sent_ok` | `sent_ok` | `SendTransaction` accepted by Fast | Real Circular success |
| `sent_err` | `sent_err` | Fast gRPC errors | Fast down / auth / protocol |

**Readout:** healthy regime → `enqueued` ≈ `sent_ok`. Persistent gap → slow Fast, drops, or errors (`sent_err` / Drops panel).

### Drops (queue / in-flight / timeout)

| Grafana field | Influx metric | Meaning | How to read |
|---------------|---------------|---------|-------------|
| `dropped_batches` | `dropped_batches` | Batches rejected (`try_send` Full / disconnected) | Queue saturated or exporter dead |
| `dropped_tx` | `dropped_transactions` | Txs lost with those batches | Same cause, tx count |
| `no_permit` | `dropped_no_permit` | `max_in_flight` semaphore saturated | Too many in-flight gRPC calls (slow Fast) |
| `timeout` | `dropped_timeout` | Call exceeded `request_timeout` | Fast / network latency |

**Readout:** ~0 under nominal load. Rising when Fast is down is **expected** (BAM stays unblocked). High with a healthy Fast → raise `queue_capacity` / `max_in_flight` or check the network.

### Queue depth / capacity / in-flight

| Grafana field | Influx metric | Meaning | How to read |
|---------------|---------------|---------|-------------|
| `queue_depth` | `queue_depth` | Current queue occupancy | Should move; near capacity → drop risk |
| `queue_capacity` | `queue_capacity` | Configured max size (default **8192**) | **Constant plateau** expected (config gauge, not a counter) |
| `in_flight` | `in_flight` | In-progress `SendTransaction` calls | Caps at `max_in_flight` (default 5120) |

**Readout:** flat `capacity` at 8192 is OK. Alert if `queue_depth` ≈ `queue_capacity` for long.

### Latency & bytes (hook_us / send_us)

| Grafana field | Influx metric | Meaning | How to read |
|---------------|---------------|---------|-------------|
| `hook_us` | `hook_us` | Time in the BAM hook (`export_bam_shared`) | Must stay **very low** vs BAM sigverify |
| `send_us` | `send_us` | Cumulative gRPC latency (µs) | Fast health / distance |
| `copy_bytes` | `copy_bytes` | Bytes copied on the hot path (BAM Arc path ≈ 0) | Arc path: often 0 |
| `sent_bytes` | `sent_bytes` | Payload accepted by Fast | App-level network volume proxy |

**Readout:** low `hook_us` = no leader regression. `sent_bytes` / `send_us` = export cost, not host CPU.

### BAM sigverify

Measurement: `bam-receive-and-buffer_sigverify-stats`

| Grafana field | Influx metric | Meaning | How to read |
|---------------|---------------|---------|-------------|
| `verify_us` | `total_verify_time_us` | Total local ed25519 time | BAM path baseline |
| `packets` | `total_packets_verified` | Packets verified | Real leader traffic |
| `p50` / `p90` | `verify_batches_pp_us_p50` / `_p90` | Verify cost percentiles | BAM perf regression |

**A/B readout:** Circular ON vs OFF → these curves should stay **stable**.

### BAM receive-and-buffer parse

Measurement: `bam-receive-and-buffer`

| Grafana field | Influx metric | Meaning | How to read |
|---------------|---------------|---------|-------------|
| `total_us` | `total_us` | Total post-sigverify parse cost | Baseline |
| `sanitization_us` | `sanitization_us` | Sanitize | Same |
| `resolution_us` | `resolution_us` | Account / ALT resolution | Same |

Only appears with BAM work (leader + batches).

### BAM connection

Measurement: `bam_connection-metrics`

| Grafana field | Influx metric | Meaning | How to read |
|---------------|---------------|---------|-------------|
| `bundle_received` | `bundle_received` | `AtomicTxnBatch` received from the BAM node | Should rise when **leader** with traffic |
| `heartbeat_received` | `heartbeat_received` | BAM heartbeats | Live connection |
| `leaderstate_sent` | `leaderstate_sent` | LeaderState sent to the node | Leader activity |
| `bundleresult_sent` | `bundleresult_sent` | Execution results sent back | Post-execute feedback |

**Readout:** out of leader → heartbeats OK, `bundle_received` ≈ 0. In leader → `bundle_received` ↑ then Circular / sigverify panels fill in.

## “It works” checklist

1. Out of leader: **connection** panel active; Circular often empty.
2. In leader: `bundle_received` ↑ → BAM sigverify/parse ↑ → Circular throughput ↑.
3. Export success: `sent_ok` tracks traffic, `dropped_*` low, `hook_us` low.
4. Fast dead: `dropped_*` / `sent_err` ↑; BAM metrics unchanged (intended).

## Host CPU / network

This dashboard does **not** show `circExporter` thread CPU% or NIC bandwidth.

- Fast app volume: `sent_bytes`, `send_us`, `in_flight`.
- Host CPU (if present): `system-stats` (`cpu_usage`) — global, not Circular-isolated.
- Thread isolation: OS tools (`pidstat -t`, `top -H`) during a leader slot.
