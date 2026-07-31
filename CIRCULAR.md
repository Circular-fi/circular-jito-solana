# Circular Fast on Jito BAM

This fork exports sigverify-verified transactions to **Circular Fast** (ARB stream) without blocking the validator leader path, from three independent hooks: the native TPU sigverify stage, the BAM post-sigverify path, and the classic (non-BAM) Jito block-engine bundle sigverify path.

## Architecture

```text
Native TPU sigverify (SigVerifyStage)   BAM Node → BamReceiveAndBuffer   Classic Jito bundle (BundleSigverifyStage)
              │                                          │                              │
              ├─ try_send(Arc) ──────┐        ┌───────── try_send(Arc) ┐   ┌──────────── try_send(Arc of a small copy)
              │  (--circular-forward-tpu)     │  (--circular-forward-preconf)           │  (--circular-forward-jito-bundle)
              └─ banking / forward   │        │  └─ parse / BamScheduler / execute       │  └─ BundleStage (execution)
                                     ▼        ▼                                          ▼
                              queue → circExporter → gRPC SendTransaction → Fast
```

- **Hot path (all three hooks):** `Arc` share (a small owned copy for bundles, see [`circular-transaction-exporter/README.md`](circular-transaction-exporter/README.md)) + non-blocking `try_send` into a bounded queue.
- **`circExporter` thread:** unconditional vote filtering (votes are never exported), exact byte-level dedup with a fixed 10s TTL (not configurable, no entry cap — see [`circular-transaction-exporter/README.md`](circular-transaction-exporter/README.md#deduplication)), wire-byte copy, unary gRPC `SendTransaction` (`forward=false`, `memo` auto-set from the transaction's ingress path — `TPU` for native TPU, `BAM_TPU`/`BAM_BUNDLE` for BAM depending on whether the `AtomicTxnBatch` is a single transaction or an atomic multi-transaction batch, `JITO_BUNDLE` for classic block-engine bundles — never operator-configurable).
- `--circular-forward-tpu`, `--circular-forward-preconf`, and `--circular-forward-jito-bundle` (all default `true`) independently gate each hook.
- Slow or dead Fast → counted drops only; no backpressure on TPU sigverify, BAM, classic bundle sigverify, or banking.

## Docs

- Grafana dashboard & metric field guide: [`grafana/README.md`](grafana/README.md)
- Dashboard JSON: [`grafana/circular-bam-exporter.json`](grafana/circular-bam-exporter.json)
