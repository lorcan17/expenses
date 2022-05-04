from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
from dotenv import load_dotenv
import json
from cryptography.fernet import Fernet
import datetime
from google.cloud import bigquery
import pandas as pd
import pytz

load_dotenv()
# Connect to GSheets
def decrypt_creds(file_path):
    ENCRYPT_KEY= os.environ['ENCRYPT_KEY']
    #keys = ast.literal_eval(GOOGLE_JSON_KEY)
    f = Fernet(ENCRYPT_KEY)
    with open(file_path, "rb") as file:
        # read the encrypted data
        encrypted_data = file.read()
        # decrypt data
    keys = f.decrypt(encrypted_data)
    #(keys)

    keys = json.loads(keys)
    #keys = ast.literal_eval(keys)
    #creds = service_account.Credentials.from_service_account_file(
    #        keys, scopes=SCOPES)
    return keys

def gsheet_connect(keys):
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = service_account.Credentials.from_service_account_info(
            keys, scopes=SCOPES)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    gsheet = service.spreadsheets()

    return gsheet

def gsheet_export(keys,spreadsheet_id,gsheet_export_range,date_format = '%Y%m%d'):
    # Does not work
    # export transaction data from google sheet
    gsheet = gsheet_connect(keys)
    spreadsheet_id = spreadsheet_id
    gsheet_export_range = gsheet_export_range
    date_format = date_format
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
    #gsheets_export =  gsheets_export.convert_dtypes()
    df['Date'] = pd.to_datetime(df['Date'] ,errors = 'coerce',format = date_format)
    df['Cost'] = pd.to_numeric(df['Cost'])
    return
    gsheet

def big_query_connect(keys):
    SCOPES = ['https://www.googleapis.com/auth/bigquery']
    creds = service_account.Credentials.from_service_account_info(
            keys, scopes=SCOPES,
    )

    client = bigquery.Client(credentials=creds, project=creds.project_id)

    return client

def big_query_load_spending(client,table_id,dataframe):
    # Example data
    #df = pd.DataFrame({'a': [1,2,4], 'b': ['123', '456', '000']})

# Define table name, in format dataset.table_name

# Load data to BQ
    #job = client.load_table_from_dataframe(df, table)
    #return
    #job.result()
    job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    #schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.

        #bigquery.SchemaField("exp_desc", bigquery.enums.SqlTypeNames.STRING),
        #bigquery.SchemaField("category_name", bigquery.enums.SqlTypeNames.STRING),
        #bigquery.SchemaField("exp_currency", bigquery.enums.SqlTypeNames.STRING),
        #bigquery.SchemaField("first_name", bigquery.enums.SqlTypeNames.STRING),
        #bigquery.SchemaField("last_name", bigquery.enums.SqlTypeNames.STRING),

        # Indexes are written if included in the schema by name.

    #],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
    )
    # Change string columns
    dataframe_dtype = pd.DataFrame(dataframe.dtypes)

    string_list = ["object","category"]
    filter_string_cols = []
    for x in dataframe_dtype[0]:
        if x in string_list:
            y = True
        else:
            y = False
        filter_string_cols.append(y)
    string_columns = dataframe_dtype[filter_string_cols]
    # Update schema for string columns only
    schema = []
    for s in string_columns.index.tolist():
        a = bigquery.SchemaField(s, bigquery.enums.SqlTypeNames.STRING)
        schema.append(a)

    job_config.schema = schema
    job = client.load_table_from_dataframe(
    dataframe, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)
