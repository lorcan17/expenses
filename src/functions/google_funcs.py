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
def decrypt_creds():
    ENCRYPT_KEY= os.environ['ENCRYPT_KEY']
    #keys = ast.literal_eval(GOOGLE_JSON_KEY)
    f = Fernet(ENCRYPT_KEY)
    with open("../encrypt_google_cloud_credentials.json", "rb") as file:
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

def big_query_connect(keys):
    SCOPES = ['https://www.googleapis.com/auth/bigquery']
    creds = service_account.Credentials.from_service_account_info(
            keys, scopes=SCOPES,
    )

    client = bigquery.Client(credentials=creds, project=creds.project_id)

    return client

def hello(client,table_id,dataframe):
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
    schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField("exp_desc", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("category_name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("exp_currency", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("first_name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("last_name", bigquery.enums.SqlTypeNames.STRING),
        
        # Indexes are written if included in the schema by name.

    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
    )

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
