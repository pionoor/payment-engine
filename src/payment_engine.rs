use crate::account::Account;
use anyhow::{anyhow, Result};
use csv::StringRecord;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;

#[derive(Debug, Clone, Serialize, Default)]
pub struct PaymentEngine {
    // (client, account)
    pub(crate) accounts: BTreeMap<u16, Account>,
    // (transaction_id, transaction)
    pub(crate) failed_transactions: Vec<String>,
    input_file_path: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
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
    #[serde(default)]
    pub(crate) amount: f32,
    #[serde(skip_serializing, skip_deserializing)]
    pub disputed: bool,
}

impl PaymentEngine {
    pub(crate) fn new(input_file_path: String) -> Self {
        Self {
            input_file_path,
            ..Default::default()
        }
    }

    fn new_file_buff_reader(&self) -> Result<csv::Reader<BufReader<File>>> {
        let file = File::open(self.input_file_path.clone())?;
        let buff_file_reader = BufReader::new(file);
        let csv_reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .trim(csv::Trim::All)
            .delimiter(b',')
            .from_reader(buff_file_reader);
        Ok(csv_reader)
    }

    // parse the transactions file and load it into a btree map.
    pub fn parse_transactions(&mut self) -> Result<()> {
        let mut csv_reader = self.new_file_buff_reader()?;

        for record in csv_reader.records() {
            match record {
                Ok(_record) => {
                    match _record.deserialize::<Transaction>(None) {
                        Ok(deserialized_record) => {
                            if deserialized_record.amount == 0.0
                                && (deserialized_record.r#type == TransactionType::Deposit
                                    || deserialized_record.r#type == TransactionType::Withdrawal)
                            {
                                self.failed_transactions
                                    .push(PaymentEngine::formatted_bad_record(
                                        &_record,
                                        anyhow!(
                                            "{:?} transaction must be above zero",
                                            deserialized_record.r#type
                                        )
                                        .into(),
                                    ));
                                // return Err(anyhow!(
                                //     "{:?} transaction must be above zero",
                                //     deserialized_record.r#type
                                // ));
                            }
                            let account = self
                                .accounts
                                .entry(deserialized_record.client)
                                .or_insert(Account {
                                    client: deserialized_record.client,
                                    available: 0.0,
                                    held: 0.0,
                                    total: 0.0,
                                    locked: false,
                                    transactions: Default::default(),
                                });
                            match account.process_transaction(&deserialized_record) {
                                Ok(_) => {}
                                Err(e) => {
                                    self.failed_transactions.push(
                                        PaymentEngine::formatted_bad_record(&_record, e.into()),
                                    );
                                }
                            }
                            if account.process_transaction(&deserialized_record).is_err() {}
                        }
                        Err(e) => {
                            self.failed_transactions
                                .push(PaymentEngine::formatted_bad_record(&_record, e.into()));
                        }
                    };
                }
                Err(e) => eprintln!("Could not read line: {}", e),
            }
        }

        Ok(())
    }

    pub(crate) fn export_accounts_to_file(&self, output_file_path: String) -> Result<()> {
        let mut wtr = csv::Writer::from_path(output_file_path)?;
        for (_, _account) in self.accounts.iter() {
            wtr.serialize(_account)?;
        }
        wtr.flush()?;
        Ok(())
    }

    pub(crate) fn export_failed_txs_to_file(
        &self,
        failed_txs_output_file_path: String,
    ) -> Result<()> {
        let mut wtr = csv::Writer::from_path(failed_txs_output_file_path)?;
        for failed_tx in self.failed_transactions.iter() {
            wtr.serialize(failed_tx)?;
        }
        wtr.flush()?;
        Ok(())
    }

    fn formatted_bad_record(record: &StringRecord, e: Box<dyn Error>) -> String {
        let bad_record = record.iter().collect::<Vec<&str>>();
        let formatted_bad_record = bad_record.join(",");
        format!("{},{}", formatted_bad_record, e)
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
