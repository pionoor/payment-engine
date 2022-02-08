use anyhow::Result;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::fs;

pub struct PaymentEngine {
    // (client, account)
    pub(crate) accounts: BTreeMap<u16, Account>,
    // (transaction_id, transaction)
    transactions: BTreeMap<u32, Transaction>, // using BtreeMap to keep the keys sorted
    failed_transactions: Vec<Transaction>,
    pub(crate) input_file_path: String,
    pub(crate) output_file_path: String,
    pub(crate) failed_txs_output_file_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub(crate) client: u16,
    #[serde(serialize_with = "float_four_digit_serialize")]
    pub(crate) available: f32,
    #[serde(serialize_with = "float_four_digit_serialize")]
    pub(crate) held: f32,
    #[serde(serialize_with = "float_four_digit_serialize")]
    pub(crate) total: f32,
    pub(crate) locked: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    ChargeBack,
    Unknown(String),
}

impl Default for TransactionType {
    fn default() -> Self {
        TransactionType::Unknown(String::default())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Transaction {
    pub(crate) r#type: TransactionType,
    pub(crate) client: u16,
    pub(crate) tx: u32,
    pub(crate) amount: f32,
}

impl PaymentEngine {
    pub(crate) fn new(
        input_file_path: &str,
        output_file_path: &str,
        failed_txs_output_file_path: &str,
    ) -> Self {
        Self {
            accounts: Default::default(),
            transactions: Default::default(),
            failed_transactions: Default::default(),
            input_file_path: input_file_path.to_string(),
            output_file_path: output_file_path.to_string(),
            failed_txs_output_file_path: failed_txs_output_file_path.to_string(),
        }
    }

    // parse the transactions file and load it into a btree map.
    fn parse_transactions(&mut self) -> Result<()> {
        let mut transactions: BTreeMap<u32, Transaction> = BTreeMap::new();
        // Build the CSV reader and iterate over each record.
        let txs_string = fs::read_to_string(self.input_file_path.to_owned())?;
        let mut rdr = csv::Reader::from_reader(txs_string.as_bytes());

        for transaction in rdr.deserialize() {
            let record: Transaction = transaction?;
            transactions.insert(record.tx, record);
        }
        self.transactions = transactions;
        Ok(())
    }

    // Reads the transactions and and process them into accounts
    pub fn process_transactions(&mut self) -> Result<()> {
        self.parse_transactions()?;
        let mut accounts: BTreeMap<u16, Account> = BTreeMap::new();
        let mut failed_transactions: Vec<Transaction> = Vec::new();

        for (tx_id, tx) in self.transactions.iter() {
            let account = accounts.entry(tx.client).or_insert(Account {
                client: tx.client,
                available: 0.0,
                held: 0.0,
                total: 0.0,
                locked: false,
            });

            match &tx.r#type {
                TransactionType::Deposit => {
                    account.available += tx.amount;
                    account.total += tx.amount;
                }
                TransactionType::Withdrawal => {
                    // Perform withdrawal if there is enough money; otherwise ignore.
                    if tx.amount <= account.total {
                        account.available -= tx.amount;
                        account.total -= tx.amount;
                    } else {
                        failed_transactions.push((*tx).clone());
                    }
                }
                TransactionType::Dispute => {
                    // Perform dispute if the original transactions exists; otherwise ignore.
                    if let Some(original_tx) = self.transactions.get(tx_id) {
                        account.available -= original_tx.amount;
                        account.held += original_tx.amount;
                    } else {
                        failed_transactions.push((*tx).clone());
                    }
                }
                TransactionType::Resolve => {
                    // Perform resolve if the original transactions exists; otherwise ignore.
                    if let Some(original_tx) = self.transactions.get(tx_id) {
                        account.available += original_tx.amount;
                        account.held -= original_tx.amount;
                    } else {
                        failed_transactions.push((*tx).clone());
                    }
                }
                TransactionType::ChargeBack => {
                    // Perform chargeBack if the original transactions exists; otherwise ignore.
                    if let Some(original_tx) = self.transactions.get(tx_id) {
                        account.total += original_tx.amount;
                        account.held -= original_tx.amount;
                        account.locked = true;
                    } else {
                        failed_transactions.push((*tx).clone());
                    }
                }
                TransactionType::Unknown(_) => {
                    failed_transactions.push((*tx).clone());
                }
            }
        }
        self.accounts = accounts;
        self.failed_transactions = failed_transactions;
        println!("A total of {} accounts were found!", &self.accounts.len());
        println!(
            "A total of {} transactions have failed!",
            &self.failed_transactions.len()
        );
        self.save_accounts_file()?;
        self.save_failed_txs_to_file()?;
        Ok(())
    }

    fn save_accounts_file(&self) -> Result<()> {
        let mut wtr = csv::Writer::from_path(self.output_file_path.clone())?;
        for (_, _account) in self.accounts.iter() {
            wtr.serialize(_account)?;
        }
        wtr.flush()?;
        Ok(())
    }

    fn save_failed_txs_to_file(&self) -> Result<()> {
        let mut wtr = csv::Writer::from_path(self.failed_txs_output_file_path.clone())?;
        for failed_tx in self.failed_transactions.iter() {
            wtr.serialize(failed_tx)?;
        }
        wtr.flush()?;
        Ok(())
    }
}

fn float_four_digit_serialize<S>(x: &f32, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let rounded = format!("{:.4}", x);
    match rounded.parse::<f32>() {
        Ok(_float) => s.serialize_f32(_float),
        Err(e) => {
            panic!("failed parsing {} into float:{}", x, e)
        }
    }
}

impl<'de> Deserialize<'de> for TransactionType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?.to_lowercase();
        let tx_type = match s.as_str() {
            "deposit" => TransactionType::Deposit,
            "withdrawal" => TransactionType::Withdrawal,
            "dispute" => TransactionType::Dispute,
            "resolve" => TransactionType::Resolve,
            "chargeback" => TransactionType::ChargeBack,
            _ => TransactionType::Unknown(s),
        };
        Ok(tx_type)
    }
}
