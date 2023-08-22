# Expenses Automation Guide

## Getting Started

1. **Save Gsheet template in your drive**: 
2. **Create BigQuery Project**: Use scripts in `src/sql/ddl` to create the tables
3. **Create Google Service Account**: Use scripts in `src/sql/ddl` and add this service account to Gsheet and BigQuery project
4. **Encrypt G Cloud Credentials**: Use `encrypt.py` to securely encrypt your Google Cloud credentials. Make sure to keep the encryption key safe.
5. **Environment Variables**: Add environment variables from the `env.example` file to your `.env` files and GitHub secrets. Refer to the [GitHub Encrypted Secrets Documentation](https://docs.github.com/en/actions/security-guides/encrypted-secrets#limits-for-secrets) for secure handling.

## Process

### Expenses

1. **Export Expenses**: Export new expenses from your Google Sheet.

2. **Import to Splitwise**: Import your expenses into Splitwise.

3. **Pull Recently Updated Expenses from Splitwise**: Export all expenses to Google BigQuery for further analysis.

4. **Import to BigQuery 

### Income

1. **Export Income**: Export all income data to Google BigQuery.

#### Cleaning

This is a tab where you define rules for cleaning the Bank Descriptions before uploading to Splitwise

### Savings

TODO

### Investment deposits

TODO

### Assets

TODO

### Update Expenses

TODO

## Gsheet Set up

TODO

## BigQuery Set up

TODO

## DBT Integration

TODO

## Google DataStudio Integration

TODO

## **Splitwise Rules**: Configure Splitwise rules based on expense descriptions to categorize expenses:

    - `.hol`: Purchased on/for a holiday -> Subcategory: Holiday
    - `pub.`: Spent at a pub -> Subcategory: Pub
    - `asset.`: Item planned for future sale (e.g., car) -> Category: Asset (value >= $500)

### Archived Rules

    -- `.self`: Purchased for self-care -> Subcategory: Self care
    - `.togo`: Purchased on the go (e.g., coffee, sandwich) -> Subcategory: To go snack/drinks
    - `.imm`: Immigration fees -> Category: Immigration Costs
    - `.work`: Work Lunches -> Subcategory: Work Lunches
    
