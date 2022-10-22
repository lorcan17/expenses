# Expenses
Automate expenses tracking

# Get set up
Encrypt G Cloud Credentials using encrypt.py
Ensure you keep key safe
Add env.example variables to .env files and github secrets
Alternatively do this: https://docs.github.com/en/actions/security-guides/encrypted-secrets#limits-for-secrets

## Spending
- Export new expenses from GoogleSheet
- Import into Splitwise
- Export all expenses into BigQuery

## Income
- Export all income into BigQuery

# Google DataStudio

## Connections
SplitWise
GoogleCloud
DBT


## DBT
brew update
brew install git
brew tap dbt-labs/dbt
brew install dbt-bigquery

pip install dbt-bigquery
sudo rm -rf /Library/Developer/CommandLineTools
 sudo xcode-select --install

 # Spltiwise  Rules
Description contains
- .hol Purchased on / for a holiday -> subcat name Holiday
- .imm immigration fees -> subcat name Immgration Costs
- .big - Big Purchase i.e whistler -> subcat name Big Purchase
- pub. - Spent at a pub ->-> subcat name Pub
- asset. - an item we plan to sell in the future i.e car -> cat name Asset

