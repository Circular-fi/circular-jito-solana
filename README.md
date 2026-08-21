# circular-transaction-exporter

Circular Fast on Jito-Solana = post-sigverify submission of every verified
transaction to **Circular Fast**, fed by three independent hooks:

- the native TPU `SigVerifyStage` (regular, non-BAM transaction flow, also
  covers relayer packets and non-bundle block-engine packets since they share
  the same sigverify stage);
- the BAM post-sigverify path (`bam_receive_and_buffer.rs`);
- the classic (non-BAM) Jito block-engine bundle path
  (`bundle_sigverify_stage.rs`).

This crate speaks the production **Fast gRPC contract** (`fast_tx.FastTx`)
directly: each verified (non-vote, by default) transaction is submitted with a
unary `SendTransaction` call, `forward = false`, so it is routed to the ARB
stream only. It observes verified traffic and feeds it to Fast; it is never a
dependency of the validator's own pipeline.

## Architecture

```text
Native TPU sigverify              BAM receive_and_buffer            Classic Jito bundle sigverify
(core/.../sigverify.rs)            (.../bam_receive_and_buffer.rs)   (core/.../bundle_sigverify_stage.rs)
    │                                   │                                   │
    ├─ ed25519_verify_serial            ├─ ed25519_verify                   ├─ ed25519_verify
    ├─ hook: export_verified            ├─ hook: export_bam_shared          ├─ hook: export_jito_bundle_shared
    │  (--circular-forward-tpu,        │  (--circular-forward-preconf,    │  (--circular-forward-jito-bundle,
    │   default true)                  │   default true)                  │   default true)
    └─ banking / forwarding stage       └─ BAM scheduler / banking          └─ BundleStage (execution)
                     │                                   │                                   │
                     └───────────────────────────────────┼───────────────────────────────────┘
                                                          ▼
                     Arc::clone of PacketBatch (TPU/BAM) or a small owned copy (bundles) + try_send (never blocks)
                                          │
                     bounded crossbeam queue (--circular-fast-queue-capacity)
                                          │  full → counted drop
                                          ▼
                     "circExporter" thread (tokio current-thread)
                                          │  filter SIMPLE_VOTE_TX unconditionally (votes are never exported)
                                          │  exact byte-level dedup, fixed 10s TTL (not configurable)
                                          │  copy wire bytes + unary SendTransaction
                                          │  bounded by --circular-fast-max-in-flight
                                          ▼
                     Fast gRPC endpoint  fast_tx.FastTx/SendTransaction
                     (forward=false, x-api-key, memo = TPU | BAM_TPU | BAM_BUNDLE | JITO_BUNDLE)
```

Guarantees:

- the critical sigverify → banking path never waits on the network on either
  hook; the hot-path export cost is an `Arc` refcount bump (wire copy is
  deferred);
- a slow or dead Fast endpoint results in counted drops (queue full, no
  in-flight permit, or per-call timeout), never in backpressure on the
  validator;
- the channel connects at exporter boot (`connect().await`) and reconnects transparently.

## Deduplication

The `circExporter` thread deduplicates transactions by exact wire bytes
before submitting them to Fast, with a fixed 10-second sliding TTL window
(`config::DEDUP_TTL`). This is a correctness safeguard against duplicate
delivery from either hook (e.g. a transaction seen as both a direct and a
forwarded packet), not an operator-tunable setting — there is no CLI flag for
it.

Implementation notes:

- exact match on the raw bytes, no bloom filter or hash truncation — zero
  false positives, zero false negatives;
- runs single-threaded on the exporter thread, so no locks or atomics are
  needed for the dedup table itself;
- bytes are stored once per distinct transaction (behind an `Rc<[u8]>` shared
  between the lookup table and the eviction queue) and the hot "duplicate"
  path never allocates;
- eviction is FIFO from the oldest entry, which is exact here because a hit
  never refreshes an entry's timestamp — insertion order equals expiration
  order;
- there is no entry cap: memory tracks the number of distinct transactions
  seen within the last 10 seconds, which is monitored via the
  `dedup_table_size` gauge (see below) rather than artificially bounded.

Metrics: `dropped_duplicates` (count of transactions dropped as duplicates)
and `dedup_table_size` (current number of distinct transactions tracked) are
reported alongside the other `circular_transaction_exporter` datapoints.

## Plug & play

```sh
agave-validator --circular-fast-api-key <key> … run
# or:  agave-validator --circular-fast-api-key-file /etc/circular/fast.key … run
# or:  CIRCULAR_FAST_API_KEY=<key> agave-validator … run
```

`--circular-fast-api-key` and `--circular-fast-api-key-file` are mutually
exclusive; prefer the file or the environment variable in production to avoid
leaking the key via the process list or shell history.

All three ingress hooks are enabled by default and can be toggled
independently:

- `--circular-forward-tpu <true|false>` (default `true`): export transactions
  verified by the native TPU sigverify stage (also covers relayer packets and
  non-bundle block-engine packets, since they share the same stage).
- `--circular-forward-preconf <true|false>` (default `true`): export
  transactions/bundles verified by the BAM post-sigverify hook.
- `--circular-forward-jito-bundle <true|false>` (default `true`): export
  classic (non-BAM) Jito block-engine bundles verified by
  `BundleSigverifyStage`.

Vote transactions are never exported on any path — there is no opt-in. The
`memo` sent with every submission is always derived from the transaction's
source (see [`TransactionSource::memo`](circular-transaction-exporter/src/event.rs)), never
operator-configurable:

| Source                                                         | Memo          |
| ---------------------------------------------------------------- | ------------- |
| Native TPU (direct, non-forwarded)                              | `TPU`         |
| Native TPU (forwarded from another validator)                   | `TPU`         |
| Single BAM transaction (`revert_on_error == false`)             | `BAM_TPU`     |
| BAM atomic multi-transaction batch (`revert_on_error == true`)  | `BAM_BUNDLE`  |
| Classic (non-BAM) Jito block-engine bundle                      | `JITO_BUNDLE` |

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
    # --circular-fast-max-in-flight <N>                     # default: 5120
    # --circular-fast-queue-capacity <N>                    # default: 8192
    # --circular-fast-request-timeout-ms <MS>               # default: 2000
    # --circular-fast-connect-timeout-ms <MS>               # default: 5000
    # --circular-forward-tpu <true|false>                   # default: true
    # --circular-forward-preconf <true|false>                # default: true
    # --circular-forward-jito-bundle <true|false>            # default: true
```

---

<p align="center">
  <a href="https://anza.xyz">
    <img alt="Anza" src="https://i.postimg.cc/VkKTnMM9/agave-logo-talc-1.png" width="250" />
  </a>
</p>

[![Build status](https://badge.buildkite.com/3a7c88c0f777e1a0fddacc190823565271ae4c251ef78d83a8.svg)](https://buildkite.com/jito/jito-solana)

# About

This repository contains Jito's fork of the Solana validator, with **Circular Fast** export of native-TPU- and BAM-verified transactions.

- Circular / BAM architecture: [`CIRCULAR.md`](CIRCULAR.md)
- Grafana metrics guide: [`grafana/README.md`](grafana/README.md)

We recommend checking out our [Gitbook](https://jito-foundation.gitbook.io/mev/jito-solana/building-the-software) for
more detailed instructions on building and running Jito-Solana.

---

## **1. Install rustc, cargo and rustfmt.**

```bash
$ curl https://sh.rustup.rs -sSf | sh
$ source $HOME/.cargo/env
$ rustup component add rustfmt
```

The `rust-toolchain.toml` file pins a specific rust version and ensures that
cargo commands run with that version. Note that cargo will automatically install
the correct version if it is not already installed.

On Linux systems you may need to install libssl-dev, pkg-config, zlib1g-dev, protobuf etc.

On Ubuntu:

```bash
$ sudo apt-get update
$ sudo apt-get install libssl-dev libudev-dev pkg-config zlib1g-dev llvm clang cmake make libprotobuf-dev protobuf-compiler libclang-dev
```

On Fedora:

```bash
$ sudo dnf install openssl-devel systemd-devel pkg-config zlib-devel llvm clang cmake make protobuf-devel protobuf-compiler perl-core libclang-dev
```

## **2. Download the source code.**

```bash
$ git clone https://github.com/jito-foundation/jito-solana.git
$ cd jito-solana
```

## **3. Build.**

```bash
$ ./cargo build
```

> [!NOTE]
> Note that this builds a debug version that is **not suitable for running a testnet or mainnet validator**. Please read [the install guide](https://docs.anza.xyz/cli/install#build-from-source) for instructions to build a release version for test and production uses.

## **4. Grant capabilities for XDP (Linux-only).**

XDP transmit is enabled on Linux by default and requires extra capabilities. After building, grant them to the validator binary:

```bash
$ sudo setcap 'cap_net_admin,cap_net_raw+eip' <path-to-agave-validator-binary>
```

For XDP zero-copy mode (`--xdp-zero-copy`), additional capabilities are needed:

```bash
$ sudo setcap 'cap_net_admin,cap_net_raw,cap_bpf,cap_perfmon+eip' <path-to-agave-validator-binary>
```

# Testing

**Run the test suite:**

```bash
$ ./cargo nextest run --profile ci  --cargo-profile ci --config-file .config/nextest.toml
```

### Starting a local testnet

Start your own testnet locally, instructions are in the [online docs](https://docs.anza.xyz/clusters/benchmark).

### Accessing the remote development cluster

* `devnet` - stable public cluster for development accessible via
devnet.solana.com. Runs 24/7. Learn more about the [public clusters](https://docs.anza.xyz/clusters)

# Benchmarking

First, install the nightly build of rustc. `cargo bench` requires the use of the
unstable features only available in the nightly build.

```bash
$ rustup install nightly
```

Run the benchmarks:

```bash
$ cargo +nightly bench
```

# Release Process

The release process for this project is described [here](RELEASE.md).

# Code coverage

To generate code coverage statistics:

```bash
$ scripts/coverage.sh
$ open target/cov/lcov-local/index.html
```

Why coverage? While most see coverage as a code quality metric, we see it primarily as a developer
productivity metric. When a developer makes a change to the codebase, presumably it's a *solution* to
some problem. Our unit-test suite is how we encode the set of *problems* the codebase solves. Running
the test suite should indicate that your change didn't *infringe* on anyone else's solutions. Adding a
test *protects* your solution from future changes. Say you don't understand why a line of code exists,
try deleting it and running the unit-tests. The nearest test failure should tell you what problem
was solved by that code. If no test fails, go ahead and submit a Pull Request that asks, "what
problem is solved by this code?" On the other hand, if a test does fail and you can think of a
better way to solve the same problem, a Pull Request with your solution would most certainly be
welcome! Likewise, if rewriting a test can better communicate what code it's protecting, please
send us that patch!

