#! /usr/bin/env python
import os

import pandas as pd
from dotenv import load_dotenv
from functions import google_funcs

load_dotenv()

spreadsheet_id = os.environ['GSHEET_SHEET_ID']
sheet_name = 'Balances Snapshot'
sheet_range = "A6:H1000"
gsheet_export_range = f'{sheet_name}!{sheet_range}'
spreadsheet_id = os.environ['GSHEET_SHEET_ID']
GSHEET_EXPORT_RANGE = 'Savings!A1:AA1000' #Edit this to be just the cell G14

keys = google_funcs.decrypt_creds("./config/encrypt_google_cloud_credentials.json")

df = google_funcs.gsheet_export(keys,spreadsheet_id,gsheet_export_range)

col = ['date', 'person', 'source', 'product','category', 'person',	'amount', 'currency']
# Unpivot data

# Convert Data types
df =  df.convert_dtypes()
df['date'] = pd.to_datetime(df['date'])
df['amount'] = df['amount'].str.replace(',', '')
df['amount'] = pd.to_numeric(df['amount'])
# Remove nulls
df = df[df.amount.notnull()]

client = google_funcs.big_query_connect(keys)

# Upload expenses
google_funcs.big_query_load_spending(
                    client,
                    table_id = "budgeting.t_balances_stage",
                    dataframe = df)
