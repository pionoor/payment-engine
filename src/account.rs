use crate::payment_engine::{Transaction, TransactionType};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize, Serializer};
use std::collections::BTreeMap;

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub(crate) client: u16,
    #[serde(serialize_with = "float_four_digit_serialize")]
    pub(crate) available: f32,
    #[serde(serialize_with = "float_four_digit_serialize")]
    pub(crate) held: f32,
    #[serde(serialize_with = "float_four_digit_serialize")]
    pub(crate) total: f32,
    pub(crate) locked: bool,
    #[serde(skip_serializing, skip_deserializing)]
    pub(crate) transactions: BTreeMap<u32, Transaction>, // using BtreeMap to keep the keys sorted
}

impl Account {
    pub fn deposit(&mut self, amount: f32) {
        self.available += amount;
        self.total += amount;
    }

    pub fn withdraw(&mut self, amount: f32) -> Result<()> {
        // Perform withdrawal if there is enough money; otherwise ignore.
        if amount <= self.total {
            self.available -= amount;
            self.total -= amount;
            Ok(())
        } else {
            Err(anyhow!("Can't withdraw; insufficient funds."))
        }
    }
    pub fn dispute(&mut self, tx_id: u32) -> Result<()> {
        // Perform dispute if the original transactions exists; otherwise ignore.
        if let Some(original_tx) = self.transactions.get_mut(&tx_id) {
            self.available -= original_tx.amount;
            self.held += original_tx.amount;
            original_tx.disputed = true;
            Ok(())
        } else {
            Err(anyhow!(
                "Can't dispute; unable to find the original transaction."
            ))
        }
    }
    pub fn resolve(&mut self, tx_id: u32) -> Result<()> {
        // Perform resolve if the original transactions exists; otherwise ignore.
        if let Some(original_tx) = self.transactions.get_mut(&tx_id) {
            if original_tx.disputed {
                self.available += original_tx.amount;
                self.held -= original_tx.amount;
                original_tx.disputed = false;
                return Ok(());
            }
            return Err(anyhow!(
                "Can't resolve; transaction is not originally disputed."
            ));
        }
        Err(anyhow!(
            "Can't resolve; unable to find the original transaction."
        ))
    }
    pub fn charge_back(&mut self, tx_id: u32) -> Result<()> {
        // Perform charge_back if the original transactions exists; otherwise ignore.
        if let Some(original_tx) = self.transactions.get_mut(&tx_id) {
            if original_tx.disputed {
                self.total += original_tx.amount;
                self.held -= original_tx.amount;
                self.locked = true;
                original_tx.disputed = false;
                return Ok(());
            }
            return Err(anyhow!(
                "Can't charge back; transaction is not originally disputed."
            ));
        }
        Err(anyhow!(
            "Can't charge back; unable to find the original transaction."
        ))
    }
    pub fn process_transaction(&mut self, transaction: &Transaction) -> Result<()> {
        if self.locked {
            return Err(anyhow!("Can not process transaction; account is locked.",));
        }

        match &transaction.r#type {
            TransactionType::Deposit => {
                self.deposit(transaction.amount);
                self.transactions
                    .insert(transaction.tx, transaction.clone());
            }
            TransactionType::Withdrawal => {
                self.withdraw(transaction.amount)?;
                self.transactions
                    .insert(transaction.tx, transaction.clone());
            }
            TransactionType::Dispute => self.dispute(transaction.tx)?,
            TransactionType::Resolve => self.resolve(transaction.tx)?,
            TransactionType::ChargeBack => self.charge_back(transaction.tx)?,
            TransactionType::Unknown(tx) => {
                return Err(anyhow!("Can't process transaction {}", tx));
            }
        }
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
