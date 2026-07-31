# Circular Fast on Jito BAM

This fork exports BAM-verified transactions to **Circular Fast** (ARB stream) without blocking the validator leader path.

## Architecture

```text
BAM Node → BamReceiveAndBuffer (sigverify)
              │
              ├─ try_send(Arc) → queue → circExporter → gRPC SendTransaction → Fast
              └─ parse / BamScheduler / execute
```

- **BAM hot path:** `Arc` share + non-blocking `try_send` into a bounded queue.
- **`circExporter` thread:** unconditional vote filtering (votes are never exported), wire-byte copy, unary gRPC `SendTransaction` (`forward=false`, `memo` auto-set to `BAM_TPU` or `BAM_BUNDLE` depending on whether the `AtomicTxnBatch` is a single transaction or an atomic multi-transaction batch — never operator-configurable).
- Slow or dead Fast → counted drops only; no backpressure on BAM / banking.

## Docs

- Grafana dashboard & metric field guide: [`grafana/README.md`](grafana/README.md)
- Dashboard JSON: [`grafana/circular-bam-exporter.json`](grafana/circular-bam-exporter.json)
