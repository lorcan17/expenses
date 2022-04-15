# Expenses
Automate expenses tracking

# Get set up
Encrypt G Cloud Credentials using encrypt.py
Ensure you keep key safe
Add env.example variables to .env files and github secrets
Alternatively do this: https://docs.github.com/en/actions/security-guides/encrypted-secrets#limits-for-secrets

## Expenses
- Export new expenses from GoogleSheet
- Import into Splitwise
- Export all expenses into BigQuery

## Income
- Export all income into BigQuery



## DBT
brew update
brew install git
brew tap dbt-labs/dbt
brew install dbt-bigquery

pip install dbt-bigquery
