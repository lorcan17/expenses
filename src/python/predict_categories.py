import os
import pickle
import numpy as np
import nltk
from dotenv import load_dotenv
from functions import google_funcs, nlp_funcs

nltk.download('punkt')
load_dotenv()


################################################################################
# READ IN GSHEET COLUMN #
################################################################################

spreadsheet_id = os.environ['GSHEET_SHEET_ID']

sheet_name = 'Expenses'
sheet_range = "A17:J1000"
gsheet_export_range = f'{sheet_name}!{sheet_range}'
DESC_EXPORT_RANGE = sheet_name+'!B17:C1300' #Edit this to be just the cell H14
CAT_IMPORT_RANGE = sheet_name+'!E18'
CONF_IMPORT_RANGE = sheet_name+'!F18'

keys = google_funcs.decrypt_creds("./config/encrypt_google_cloud_credentials.json")
gsheet = google_funcs.gsheet_connect(keys)

df = google_funcs.gsheet_export(keys,spreadsheet_id,DESC_EXPORT_RANGE)

df =  df.convert_dtypes()
df['Description'] = df['Description'].str.title()
df['Description'] = df['Description'] + ' ' + df['Category Codes']
df['Description'] = df['Description'].str.strip()
if df.empty:
    print("No descriptions added")
    exit()



dtypes = {col: 'str' for col in df.columns}

################################################################################
# MAKE PREDICTIONS #
################################################################################

vectorizer = pickle.load(open("data/vectorizer.pickle", "rb"))
model = pickle.load(open("data/model.pickle", "rb"))

descriptions = df['Description']
descriptions = nlp_funcs.get_nlp_ready(descriptions)
descriptions = vectorizer.transform(descriptions)
df['categories'] = model.predict(descriptions)
proba = model.predict_proba(descriptions)
max_proba = np.amax(proba, axis=1)
df['confidence'] = max_proba

def import_column(df,col,spreadsheet_id, gsheet_import_range):

    data = [df[col].values.tolist()]
    value_range_body = {"values": data,
                    "majorDimension": "COLUMNS"}

    gsheet.values().update(spreadsheetId=spreadsheet_id,
                            range=gsheet_import_range,
                            valueInputOption='RAW',
                            body = value_range_body
                            ).execute()

import_column(df,'categories',spreadsheet_id, CAT_IMPORT_RANGE)

import_column(df,'confidence',spreadsheet_id, CONF_IMPORT_RANGE)
