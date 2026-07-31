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
                     │  filter SIMPLE_VOTE_TX unconditionally (votes are never exported)
                     │  copy wire bytes + unary SendTransaction
                     │  bounded by --circular-fast-max-in-flight
                     ▼
        Fast gRPC endpoint  fast_tx.FastTx/SendTransaction
        (forward=false, x-api-key, memo = BAM_TPU | BAM_BUNDLE)
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
agave-validator --circular-fast-api-key <key> … run
# or:  agave-validator --circular-fast-api-key-file /etc/circular/fast.key … run
# or:  CIRCULAR_FAST_API_KEY=<key> agave-validator … run
```

`--circular-fast-api-key` and `--circular-fast-api-key-file` are mutually
exclusive; prefer the file or the environment variable in production to avoid
leaking the key via the process list or shell history.

Only BAM-received transactions are exported (not the vanilla TPU / relayer
path). Vote transactions are never exported — there is no opt-in. The `memo`
sent with every submission is always derived from the transaction's source
(see [`TransactionSource::memo`](src/event.rs)), never operator-configurable:

| Source                                      | Memo        |
| -------------------------------------------- | ----------- |
| Single BAM transaction (`revert_on_error == false`) | `BAM_TPU`   |
| BAM atomic multi-transaction batch (`revert_on_error == true`) | `BAM_BUNDLE` |

## Validator BAM Script example

Unlike a non-voting RPC node, a BAM validator must be electable as leader for
BAM to hand it transactions to assemble: it therefore needs `--vote-account`
(no `--no-voting`/`--private-rpc`), the `--bam-url` connection, and the
`tip-*` flags, which become **mandatory** as soon as voting is active (see
`validator/src/commands/run/execute.rs::tip_manager_config_from_matches`).
Replace every `<...>` placeholder with your real values.

```bash
#!/bin/bash
exec /home/user/circular-jito-solana/target/release/agave-validator \
    --identity /home/user/validator-keypair.json \
    --vote-account <YOUR_VOTE_ACCOUNT_PUBKEY> \
    --known-validator 7Np41oeYqPefeNQEHSv1UDhYrehxin3NStELsSKCT4K2 \
    --known-validator GdnSyH3YtwcxFvQrVVJMm1JhTS4QVX7MFsX56uJLUfiZ \
    --known-validator DE1bawNcRJB9rVm3buyMVfr8mBEoyyu73NBovf2oXJsJ \
    --known-validator CakcnaRDHka2gXyfbEd2d3xsvkJkqsLw2akB3zsN1D2S \
    --only-known-rpc \
    --ledger /mnt/ledger \
    --accounts /mnt/accounts \
    --log /root/solana-validator.log \
    --rpc-port 8899 \
    --rpc-bind-address 0.0.0.0 \
    --full-rpc-api \
    --dynamic-port-range 8000-8040 \
    --entrypoint entrypoint.mainnet-beta.solana.com:8001 \
    --entrypoint entrypoint2.mainnet-beta.solana.com:8001 \
    --entrypoint entrypoint3.mainnet-beta.solana.com:8001 \
    --entrypoint entrypoint4.mainnet-beta.solana.com:8001 \
    --entrypoint entrypoint5.mainnet-beta.solana.com:8001 \
    --expected-genesis-hash 5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d \
    --wal-recovery-mode skip_any_corrupted_record \
    --limit-ledger-size \
    --minimal-snapshot-download-speed 92428800 \
    --enable-scheduler-bindings \
    --bam-url <YOUR_BAM_NODE_URL> \
    --tip-payment-program-pubkey <TIP_PAYMENT_PROGRAM_PUBKEY> \
    --tip-distribution-program-pubkey <TIP_DISTRIBUTION_PROGRAM_PUBKEY> \
    --merkle-root-upload-authority <MERKLE_ROOT_UPLOAD_AUTHORITY_PUBKEY> \
    --commission-bps <COMMISSION_BPS> \
    --circular-fast-api-key <YOUR_API_KEY> \
    --circular-fast-url http://fra.cashback.circular.fi \
    --circular-fast-cashback-address <YOUR_PUBKEY_CASHBACK_WALLET>
    # Optional, otherwise defaults apply:
    # --authorized-voter <PATH_KEYPAIR>                     # default: the --identity key
    # --block-engine-url <URL_BLOCK_ENGINE>                 # classic fallback if BAM disconnects
    # --relayer-url <URL_RELAYER>                           # same, see BAM/classic coexistence
    # --circular-fast-url <URL_FAST>                        # default: http://cashback.circular.fi
    # --circular-fast-max-in-flight <N>                     # default: 1024
    # --circular-fast-queue-capacity <N>                    # default: 8192
    # --circular-fast-request-timeout-ms <MS>               # default: 2000
    # --circular-fast-connect-timeout-ms <MS>               # default: 5000
```
