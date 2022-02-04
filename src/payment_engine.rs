use anyhow::Result;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::BTreeMap;
use std::fs;

pub struct PaymentEngine {
    pub accounts: BTreeMap<u16, Account>,
    transactions: BTreeMap<u32, Transaction>,
    failed_transactions: Vec<Transaction>,
    pub input_file_path: String,
    pub output_file_path: String,
    pub failed_txs_output_file_path: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    ChargeBack,
    Void,
}

impl Default for TransactionType {
    fn default() -> Self {
        TransactionType::Void
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

    fn parse_transactions(&mut self) -> Result<()> {
        let mut transactions: BTreeMap<u32, Transaction> = BTreeMap::new();
        // Build the CSV reader and iterate over each record.
        let foo = fs::read_to_string(self.input_file_path.to_owned())?;
        let mut rdr = csv::Reader::from_reader(foo.as_bytes());

        for transaction in rdr.deserialize() {
            let record: Transaction = transaction?;
            transactions.insert(record.tx, record);
        }
        self.transactions = transactions;
        Ok(())
    }

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

            match tx.r#type {
                TransactionType::Deposit => {
                    account.available = account.available + tx.amount;
                    account.total = account.total + tx.amount;
                }
                TransactionType::Withdrawal => {
                    // Only perform withdrawal if there is enough money; otherwise ignore.
                    if tx.amount <= account.total {
                        account.available = account.available - tx.amount;
                        account.total = account.total - tx.amount;
                    } else {
                        failed_transactions.push((*tx).clone());
                    }
                }
                TransactionType::Dispute => {
                    if let Some(original_tx) = self.transactions.get(&tx_id) {
                        account.available = account.available - original_tx.amount;
                        account.held = account.held + original_tx.amount;
                    } else {
                        failed_transactions.push((*tx).clone());
                    }
                }
                TransactionType::Resolve => {
                    if let Some(original_tx) = self.transactions.get(&tx_id) {
                        account.available = account.available + original_tx.amount;
                        account.held = account.held - original_tx.amount;
                    } else {
                        failed_transactions.push((*tx).clone());
                    }
                }
                TransactionType::ChargeBack => {
                    if let Some(original_tx) = self.transactions.get(&tx_id) {
                        account.total = account.total + original_tx.amount;
                        account.held = account.held - original_tx.amount;
                        account.locked = true;
                    } else {
                        failed_transactions.push((*tx).clone());
                    }
                }
                TransactionType::Void => {}
            }
        }
        self.accounts = accounts;
        self.failed_transactions = failed_transactions;

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
