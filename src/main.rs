mod payment_engine;

use crate::payment_engine::PaymentEngine;
use std::env;

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() < 3 {
        panic!("missing argument, example: transactions.csv > accounts.csv ")
    }
    let transaction_file_path = format!("./csvFiles/{}", args[0]);
    let account_file_path = format!("./csvFiles/{}", args[2]);
    let failed_txs_file_path = format!("./csvFiles/{}", "failed.csv");
    let mut engine = PaymentEngine::new(
        &transaction_file_path,
        &account_file_path,
        &failed_txs_file_path,
    );
    engine
        .process_transactions()
        .expect("Failed at processing transactions");
    println!("transactions processing complete!")
}
