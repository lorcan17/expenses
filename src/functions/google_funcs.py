import os
import json
from dotenv import load_dotenv
from cryptography.fernet import Fernet
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
#import pytz
# Other
import nltk
from nltk.stem import PorterStemmer

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

def gsheet_export(keys,spreadsheet_id,gsheet_export_range):
    # Does not work
    # export transaction data from google sheet
    gsheet = gsheet_connect(keys)
    result = gsheet.values().get(spreadsheetId=spreadsheet_id,
                                range=gsheet_export_range).execute()
    values = result.get('values', [])

    # Format as DF and promote first row as headers
    gsheet_df = pd.DataFrame(values)
    header_row = 0
    gsheet_df.columns = gsheet_df.iloc[header_row]
    gsheet_df = gsheet_df.drop(header_row)
    gsheet_df = gsheet_df.reset_index(drop=True)
    return gsheet_df

def big_query_connect(keys):
    SCOPES = ['https://www.googleapis.com/auth/bigquery']
    creds = service_account.Credentials.from_service_account_info(
            keys, scopes=SCOPES,
    )

    client = bigquery.Client(credentials=creds, project=creds.project_id)

    return client

def big_query_export(keys,query):
    client = big_query_connect(keys)
    query_job = client.query(query).to_dataframe()
    return query_job

def big_query_load_spending(client,table_id,dataframe,write_disposition = "WRITE_TRUNCATE"):
    # Example data
    #df = pd.DataFrame({'a': [1,2,4], 'b': ['123', '456', '000']})

# Define table name, in format dataset.table_name

# Load data to BQ
    #job = client.load_table_from_dataframe(df, table)
    #return
    #job.result()
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

def get_nlp_ready(descriptions):
    tokenized_descriptions = descriptions.str.lower()
    tokenized_descriptions = tokenized_descriptions.apply(nltk.word_tokenize)

    # Use NLTK's Porter stemmer to stem the tokens
    stemmer = PorterStemmer()
    stemmed_tokens= tokenized_descriptions.apply(lambda d : [stemmer.stem(t) for t in d])

    # Join tokens into one string
    stemmed_tokens = stemmed_tokens.apply(lambda x: ' '.join(x))

    return stemmed_tokens
