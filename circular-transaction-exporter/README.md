# circular-transaction-exporter

Circular Fast on Jito-Solana = BAM post-sigverify submission of every verified
transaction to **Circular Fast** immediately after BAM signature verification.

This crate speaks the production **Fast gRPC contract** (`fast_tx.FastTx`)
directly: each verified (non-vote, by default) BAM transaction is submitted
with a unary `SendTransaction` call, `forward = false`, so it is routed to the
ARB stream only. It observes BAM-verified traffic and feeds it to Fast; it is
never a dependency of the validator's own pipeline.

## Architecture

```text
BAM receive_and_buffer (core/.../bam_receive_and_buffer.rs)
    │
    ├─ ed25519_verify
    ├─ Circular hook: Arc::clone of PacketBatch + try_send (never blocks)
    └─ BAM scheduler / banking immediately (unchanged; reads same Arc)
                     │
                     ▼
        bounded crossbeam queue (--circular-fast-queue-capacity)
                     │  full → counted drop
                     ▼
        "circExporter" thread (tokio current-thread)
                     │  filter SIMPLE_VOTE_TX unless --circular-fast-include-votes
                     │  copy wire bytes + unary SendTransaction
                     │  bounded by --circular-fast-max-in-flight
                     ▼
        Fast gRPC endpoint  fast_tx.FastTx/SendTransaction
        (forward=false, x-api-key)
```

Guarantees:

- the critical BAM sigverify → banking path never waits on the network; the
  hot-path export cost is an `Arc` refcount bump (wire copy is deferred);
- a slow or dead Fast endpoint results in counted drops (queue full, no
  in-flight permit, or per-call timeout), never in backpressure on the
  validator;
- the channel connects lazily and reconnects transparently.

## Plug & play

```sh
agave-validator --circular-fast-api-key-file /etc/circular/fast.key … run
# or:  CIRCULAR_FAST_API_KEY=<key> agave-validator … run
```

Only BAM-received transactions are exported (not the vanilla TPU / relayer path).
