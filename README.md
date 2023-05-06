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
- .self purchased for self care -> subcat Self care
- .togo purchased on the go i.e coffee or sandwhich to go ->  subcat nameTo go snack / drinks
- .imm immigration fees -> cat name Immgration Costs
- pub. - Spent at a pub ->-> subcat name Pub
- asset. - an item we plan to sell in the future i.e car -> cat name Asset (no less than $500 value)

# Environment
Update environment:

`conda env export --from-history  > environment.yml`
`conda env export --no-builds | grep -v "prefix" > environment.yml`


Create environment

`conda env create -f environment.yml`

use pip freeze
`pip list --format=freeze > requirements.txt`



delete the test group
https://splitwise.readthedocs.io/en/latest/user/example.html?highlight=group#creating-a-new-group