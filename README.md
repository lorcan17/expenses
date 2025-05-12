# Personal Finance Tracker

A comprehensive system for tracking, categorizing, and analyzing personal financial data using Google Sheets, GitHub Actions, BigQuery, and AI-powered analytics.

## System Overview

This project creates an end-to-end pipeline for personal finance management:

1. **Data Collection**: Store bank transactions in Google Sheets
2. **AI Categorization**: Use LLM-powered categorization to automatically classify transactions
3. **Data Warehouse**: Push processed data to BigQuery for robust storage and analysis
4. **Analytics**: Visualize spending patterns with BigQuery dashboards
5. **Natural Language Queries**: Ask questions about your finances in plain English using Gemini API

## Architecture

```
┌─────────────┐     ┌───────────────┐     ┌───────────────┐     ┌────────────┐     ┌──────────────┐
│  Bank Data  │────▶│ Google Sheets │────▶│ GitHub Action │────▶│  BigQuery  │────▶│ Dashboards & │
│ (CSV/Excel) │     │  (with tabs)  │     │  (Python/dbt) │     │ Data Tables│     │ Gemini NL UI │
└─────────────┘     └───────────────┘     └───────────────┘     └────────────┘     └──────────────┘
                            │                                        ▲
                            │                                        │
                            └────────────────────────────────────────┘
                                   (Direct BQ API connection)
```

## Setup Instructions

### 1. Google Sheet Configuration

1. Create a new Google Sheet with tabs for each bank account/credit card
2. Set up the column structure to align with each bank export
4. Add the following columns to allow AI powered categorisation
   - Category (will be filled by AI)
   - Subcategory (will be filled by AI)
   - Reasoning (will be filled by AI)
4. Add the Apps Script for GitHub Actions integration (see [Google Apps Script Setup](#google-apps-script-setup))

### 2. Repository Setup

1. Clone this repository to your local machine
   ```bash
   git clone https://github.com/yourusername/personal-finance-tracker.git
   cd personal-finance-tracker
   ```

2. Set up the required GitHub repository secrets:
   - `GOOGLE_SHEET_ID` - ID of your Google Sheet
   - `GOOGLE_CREDENTIALS` - Service account JSON for BigQuery access
   - `OPENAI_API_KEY` - Your OpenAI API key for transaction categorization
   - `GEMINI_API_KEY` - Google Gemini API key for natural language queries

### 3. Google Apps Script Setup

1. In your Google Sheet, go to Extensions > Apps Script
2. Copy the code from `scripts/apps-script/trigger.js` into the script editor
3. Save and authorize the script
4. Refresh your Google Sheet to see the new "Finance Pipeline" menu

### 4. BigQuery Setup

1. Create a new BigQuery project (or use an existing one)
2. Create a dataset for your financial data:
   ```sql
   CREATE DATASET IF NOT EXISTS personal_finance;
   ```
3. The dbt models will handle table creation automatically

### 5. First Run

1. Download transaction data from your banks (CSV/Excel format)
2. Copy/paste the data into the appropriate tabs in your Google Sheet
3. In the Google Sheet, use the "Finance Pipeline > Categorize Transactions" menu
4. Review and correct any categorizations as needed
5. Use "Finance Pipeline > Update BigQuery Data" to load the data

## Workflow

### Categorizing New Transactions

1. Download new transactions from your bank as CSV/Excel
2. Add them to the appropriate tab in your Google Sheet
3. Click "Finance Pipeline > Categorize Transactions"
4. The GitHub Action will:
   - Run the Python categorization script
   - Use AI to categorize transactions based on descriptions and amounts
   - Update the Google Sheet with categories and subcategories

### Updating BigQuery Data

1. Review the AI-suggested categories and make any corrections
2. Check the "Confirmed" box for reviewed transactions
3. Click "Finance Pipeline > Update BigQuery Data"
4. The GitHub Action will:
   - Run dbt to process and model the data
   - Load it into BigQuery tables
   - Refresh the dashboards

### Analyzing Your Finances

Access your financial data in several ways:
1. Built-in BigQuery dashboards for spending patterns, trends, and insights
2. SQL queries for custom analysis
3. Natural language questions using the Gemini-powered interface

## Project Structure

```
├── .github/
│   └── workflows/
│       ├── categorize.yml    # GitHub Action for transaction categorization
│       └── update_bq.yml     # GitHub Action for BigQuery updates via dbt
├── scripts/
│   ├── apps-script/          # Google Apps Script code for Sheet integration
│   └── categorization/       # Python scripts for AI categorization
├── dbt/                      # dbt project for data modeling
│   ├── models/               # SQL models for transforming raw data
│   ├── macros/               # Reusable SQL components
│   └── dbt_project.yml       # dbt project configuration
├── dashboard/                # Dashboard configuration and setup
└── gemini/                   # Gemini API integration for natural language queries
```

## AI Categorization

The categorization system uses OpenAI's API to intelligently classify transactions based on:
- Transaction description
- Transaction amount
- Previous categorization patterns
- Known merchant categories

The system uses a predefined category structure:
- Categories: Main spending groups (e.g., "Housing", "Food", "Transportation")
- Subcategories: Specific spending types (e.g., "Rent", "Groceries", "Public Transit")

This hierarchical categorization allows for both high-level trend analysis and detailed spending insights.

## dbt Data Models

The dbt project includes models for:
- Cleaned and standardized transaction data
- Monthly spending aggregations by category
- YTD financial summaries
- Budget vs. actual comparisons
- Predictive spending forecasts

## Natural Language Queries with Gemini

The Gemini integration allows you to ask questions about your finances in plain English:
- "How much did I spend on restaurants last month?"
- "What's my average monthly grocery spending this year?"
- "Show me my biggest spending categories in Q1"
- "Am I spending more on entertainment compared to last year?"

## Contributing

Contributions to improve the system are welcome! Please follow these steps:
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Inspired by many open source personal finance tools
- Uses OpenAI for transaction categorization
- Leverages dbt for data transformation
- Built on BigQuery for scalable analytics