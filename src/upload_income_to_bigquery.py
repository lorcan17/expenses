import os
import pandas as pd # pylint: disable=import-error
from dotenv import load_dotenv
from functions import google_funcs
load_dotenv()

spreadsheet_id = os.environ['GSHEET_SHEET_ID']
GSHEET_EXPORT_RANGE = 'Income!B6:F1000' #Edit this to be just the cell G14

keys = google_funcs.decrypt_creds("./encrypt_google_cloud_credentials.json")
df = google_funcs.gsheet_export(keys,spreadsheet_id,GSHEET_EXPORT_RANGE)

# Convert Data types
df =  df.convert_dtypes()
df['date'] = pd.to_datetime(df['date'] ,errors = 'coerce',format = '%d/%m/%Y')
df['amount'] = df['amount'].str.replace(',', '')
df['amount'] = pd.to_numeric(df['amount'])

client = google_funcs.big_query_connect(keys)

# Upload expenses
google_funcs.big_query_load_spending(
                    client,
                    table_id = "budgeting.gsheet_income",
                    dataframe = df)
