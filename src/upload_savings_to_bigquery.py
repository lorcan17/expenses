from functions import google_funcs
import pandas as pd

spreadsheet_id = '1CpbYfhi6bbXz5oqMs6mJs4y5ETlq2-mSCkLaU_9Wo68'
gsheet_export_range = 'Savings!A1:F1000' #Edit this to be just the cell G14

keys = google_funcs.decrypt_creds("./encrypt_google_cloud_credentials.json")
gsheet = google_funcs.gsheet_connect(keys)

result = gsheet.values().get(spreadsheetId=spreadsheet_id,
                            range=gsheet_export_range).execute()
values = result.get('values', [])
    # Format as DF and promote first row as headers
df = pd.DataFrame(values)
header_row = 0
df.columns = df.iloc[header_row]
df = df.drop(header_row)
df = df.reset_index(drop=True)

# Convert Data types
df =  df.convert_dtypes()
df['date'] = pd.to_datetime(df['date'] ,errors = 'coerce',format = '%d/%m/%Y')
df['amount'] = pd.to_numeric(df['amount'])

client = google_funcs.big_query_connect(keys)

# Upload expenses
google_funcs.big_query_load_spending(
                    client,
                    table_id = "budgeting.gsheet_savings",
                    dataframe = df)
