mod account;
mod payment_engine;

use crate::payment_engine::PaymentEngine;
use std::env;

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();

    let transaction_file_path = format!("./csvFiles/{}", args[0]);
    let account_file_path = format!("./csvFiles/{}", "accounts.csv");
    let failed_txs_file_path = format!("./csvFiles/{}", "failed.csv");
    let mut engine = PaymentEngine::new(transaction_file_path);
    engine
        .parse_transactions()
        .expect("Failed at processing transactions");
    engine
        .export_accounts_to_file(account_file_path)
        .expect("exporting account to file failed.");
    engine
        .export_failed_txs_to_file(failed_txs_file_path)
        .expect("exporting failed transactions to file failed.");
    println!("A total of {} accounts were found!", &engine.accounts.len());
    println!(
        "A total of {} transactions have failed!",
        &engine.failed_transactions.len()
    );
    println!("transactions processing complete!")
}
