## Payment Engine

A simple toy payments engine that reads a series of transactions from a CSV file, updates client accounts, handles disputes
and chargebacks, and then outputs the state of clients accounts as a CSV file.

### How to Run
`cargo run -- transactions.csv > accounts.csv`.

`transactions.csv` is the name of the csv file that exists in the `./csvFiles`. It contains a series of transactions to be 
read and processed.

`accounts.csv` is the name of the file that exists in the `./csvFiles`. It would contain the accounts details 
as a result processing the transactions.

### Notes

- Both of the csv files must exist before running the app. It does not create new ones if one or both of those file do 
not exist.
- The name of those csv files must match with name of the files that are passed in the arguments.
- An extra file `failed.csv` will contain those failed transactions, each with an err message. This might be useful in case we need to deal with them later.

