//! Send real signed transactions to a Circular Fast gRPC endpoint through the
//! exact `fast_tx.FastTx/SendTransaction` contract the validator uses
//! (`forward = false`, `x-api-key`).
//!
//! Intended to be run from a host allowed to reach the Fast gRPC port. It
//! builds a well-formed, signed transaction (recent mainnet blockhash,
//! ephemeral keypair) and submits it; the transaction is routed to the ARB
//! stream only, never executed on-chain. It prints the signature (tx_id) so
//! it can be looked up in `circular_arb.logs_minimal`.
//!
//! ```text
//! fast-send \
//!     --url http://cashback.circular.fi \
//!     --api-key <KEY> \
//!     [--rpc https://api.mainnet-beta.solana.com] \
//!     [--count 1] [--memo "circular test"]
//! ```

#![allow(clippy::arithmetic_side_effects)]

use {
    circular_transaction_exporter::proto::{SendTransactionRequest, fast_tx_client::FastTxClient},
    solana_keypair::Keypair,
    solana_rpc_client::rpc_client::RpcClient,
    solana_signer::Signer,
    solana_transaction::versioned::VersionedTransaction,
    std::time::Duration,
    tonic::{Request, metadata::MetadataValue, transport::Endpoint},
};

struct Args {
    url: String,
    api_key: String,
    rpc: String,
    count: usize,
    memo: Option<String>,
    dry_run: bool,
}

fn parse_args() -> Args {
    let mut url = "http://cashback.circular.fi".to_string();
    let mut api_key = std::env::var("CIRCULAR_FAST_API_KEY").unwrap_or_default();
    let mut rpc = "https://api.mainnet-beta.solana.com".to_string();
    let mut count = 1usize;
    let mut memo = None;
    let mut dry_run = false;

    let mut iter = std::env::args().skip(1);
    while let Some(flag) = iter.next() {
        let mut value = |flag: &str| {
            iter.next()
                .unwrap_or_else(|| fail(&format!("{flag} requires a value")))
        };
        match flag.as_str() {
            "--url" => url = value("--url"),
            "--api-key" => api_key = value("--api-key"),
            "--rpc" => rpc = value("--rpc"),
            "--count" => {
                let raw = value("--count");
                count = raw
                    .parse()
                    .unwrap_or_else(|err| fail(&format!("invalid --count {raw}: {err}")));
            }
            "--memo" => memo = Some(value("--memo")),
            "--dry-run" => dry_run = true,
            "--help" | "-h" => {
                eprintln!(
                    "usage: fast-send [--url URL] [--api-key KEY] [--rpc URL] [--count N] \
                     [--memo STRING] [--dry-run]\n(the API key can also come from \
                     CIRCULAR_FAST_API_KEY; --dry-run builds and prints the signed tx as \
                     base58/base64 without sending over gRPC)"
                );
                std::process::exit(0);
            }
            other => fail(&format!("unknown flag: {other}")),
        }
    }

    if !dry_run && api_key.is_empty() {
        fail("an API key is required (--api-key or CIRCULAR_FAST_API_KEY)");
    }
    Args {
        url,
        api_key,
        rpc,
        count: count.max(1),
        memo,
        dry_run,
    }
}

fn fail(message: &str) -> ! {
    eprintln!("error: {message}");
    std::process::exit(1);
}

/// A well-formed, signed transaction: a self-transfer of 0 lamports from an
/// ephemeral keypair. It is never executed (forward=false routes it to the
/// ARB stream only), so the empty balance is irrelevant; the point is a valid
/// signature and wire encoding.
fn build_transaction(blockhash: solana_hash::Hash) -> VersionedTransaction {
    let payer = Keypair::new();
    let legacy = solana_system_transaction::transfer(&payer, &payer.pubkey(), 0, blockhash);
    VersionedTransaction::from(legacy)
}

#[tokio::main]
async fn main() {
    let args = parse_args();

    eprintln!("fast-send: fetching a recent blockhash from {}", args.rpc);
    let rpc = RpcClient::new(args.rpc.clone());
    let blockhash = rpc.get_latest_blockhash().unwrap_or_else(|err| {
        fail(&format!(
            "failed to fetch blockhash from {}: {err}",
            args.rpc
        ))
    });

    // Dry-run: build and print the signed transaction (base58 = default
    // sendTransaction encoding, and base64) without any gRPC connection, so
    // it can be submitted via the HTTP JSON-RPC sendTransaction endpoint.
    if args.dry_run {
        for index in 0..args.count {
            let transaction = build_transaction(blockhash);
            let signature = transaction.signatures[0].to_string();
            let wire = bincode::serialize(&transaction)
                .unwrap_or_else(|err| fail(&format!("failed to serialize transaction: {err}")));
            println!("[{index}] tx_id (signature) : {signature}");
            println!("      base58 : {}", bs58::encode(&wire).into_string());
        }
        return;
    }

    eprintln!("fast-send: connecting to {}", args.url);
    let channel = Endpoint::from_shared(args.url.clone())
        .unwrap_or_else(|err| fail(&format!("invalid --url {}: {err}", args.url)))
        .connect_timeout(Duration::from_secs(10))
        .tcp_nodelay(true)
        .connect()
        .await
        .unwrap_or_else(|err| fail(&format!("failed to connect to {}: {err}", args.url)));
    let mut client = FastTxClient::new(channel);

    let api_key: MetadataValue<_> = args
        .api_key
        .parse()
        .unwrap_or_else(|err| fail(&format!("invalid API key: {err}")));

    let mut accepted = 0usize;
    for index in 0..args.count {
        let transaction = build_transaction(blockhash);
        let signature = transaction.signatures[0].to_string();
        let wire = bincode::serialize(&transaction)
            .unwrap_or_else(|err| fail(&format!("failed to serialize transaction: {err}")));

        println!("[{index}] tx_id (signature) : {signature}");
        println!("      wire_size          : {}", wire.len());
        println!(
            "      wire_sha256        : {}",
            hex::encode(solana_sha256_hasher::hash(&wire).to_bytes())
        );

        let mut request = Request::new(SendTransactionRequest {
            transaction: wire,
            memo: args.memo.clone(),
            forward: Some(false),
            cashback_address: None,
        });
        request.metadata_mut().insert("x-api-key", api_key.clone());

        match client.send_transaction(request).await {
            Ok(response) => {
                let response = response.into_inner();
                accepted += 1;
                println!("      -> accepted");
                println!("         response.signature  : {}", response.signature);
                println!("         response.request_id : {}", response.request_id);
                println!("         response.bundle_id  : {:?}", response.bundle_id);
            }
            Err(status) => {
                println!(
                    "      -> rejected [{}]: {}",
                    status.code(),
                    status.message()
                );
            }
        }
    }

    eprintln!(
        "fast-send: done, {accepted}/{} accepted by the Fast endpoint",
        args.count
    );
}
