import json
import os

#import pytz
# Other
import pandas as pd
from cryptography.fernet import Fernet
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build


load_dotenv()
# Connect to GSheets
def decrypt_creds(file_path):
    encrypt_key = os.environ['ENCRYPT_KEY']
    #keys = ast.literal_eval(GOOGLE_JSON_KEY)
    fernet = Fernet(encrypt_key)
    with open(file_path, "rb") as file:
        # read the encrypted data
        encrypted_data = file.read()
        # decrypt data
    keys = fernet.decrypt(encrypted_data)
    keys = json.loads(keys)
    return keys

def gsheet_connect(keys):
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = service_account.Credentials.from_service_account_info(
            keys, scopes=SCOPES)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    gsheet = service.spreadsheets()

    return gsheet

def gsheet_export(keys,spreadsheet_id,gsheet_export_range, export_as = "df"):
    # export transaction data from google sheet
    gsheet = gsheet_connect(keys)
    result = gsheet.values().get(spreadsheetId=spreadsheet_id,
                                range=gsheet_export_range).execute()
    values = result.get('values', [])
    if export_as == 'dict':
        result_dict = {}
        for pair in values:
            
            try:
                key = pair[0]
                value = pair[1]
                result_dict[key] = value
            except:
                continue       
        return result_dict
    elif export_as == 'df':
        # Format as DF and promote first row as headers
        gsheet_df = pd.DataFrame(values)
        header_row = 0
        gsheet_df.columns = gsheet_df.iloc[header_row]
        gsheet_df = gsheet_df.drop(header_row)
        gsheet_df = gsheet_df.reset_index(drop=True)
        gsheet_df.replace('', pd.NA, inplace=True)
        return gsheet_df
    
def get_config(spreadsheet_id, sheet_name, range):
    spreadsheet_id = "1QVfZVyLSsMksl2xiSHMHPWBK5ITHqs53F0vQxqlcuak"
    export_range = f"{sheet_name}!{range}"
    keys = decrypt_creds("./config/encrypt_google_cloud_credentials.json")
    config = gsheet_export(keys,spreadsheet_id,export_range, export_as = 'dict')

    return config

def big_query_connect(keys):
    SCOPES = ['https://www.googleapis.com/auth/bigquery']
    creds = service_account.Credentials.from_service_account_info(
            keys, scopes=SCOPES,
    )

    client = bigquery.Client(credentials=creds, project=creds.project_id)

    return client

def big_query_export(keys,query, query_is_file_path = False):
    if query_is_file_path:
        query = read_text_from_file(query)
    client = big_query_connect(keys)
    query_job = client.query(query).to_dataframe()
    return query_job

def read_text_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            text = file.read()
        return text
    except FileNotFoundError:
        print("File not found.")
    except IOError:
        print("Error reading the file.")

def big_query_query(keys, query, query_is_file_path = False):
    if query_is_file_path:
        query = read_text_from_file(query)
    client = big_query_connect(keys)
    query_job = client.query(query)
    rows = query_job.result() 
    if query_job.statement_type == 'MERGE':
        print(query_job.dml_stats)
    return query_job

def big_query_load_spending(client,table_id,dataframe,write_disposition = "WRITE_TRUNCATE"):

    job_config = bigquery.LoadJobConfig(
    write_disposition=write_disposition,
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
    f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

def import_column(keys, df,col,spreadsheet_id, gsheet_import_range):
    gsheet = gsheet_connect(keys)
    data = [df[col].values.tolist()]
    value_range_body = {"values": data,
                    "majorDimension": "COLUMNS"}

    gsheet.values().update(spreadsheetId=spreadsheet_id,
                            range=gsheet_import_range,
                            valueInputOption='RAW',
                            body = value_range_body
                            ).execute()