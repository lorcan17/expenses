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
test_run = os.environ['TEST_RUN']
SHEET_NAME = "Splitwise Bulk Import" if test_run == "No" else "Splitwise Bulk Import Test"
DESC_EXPORT_RANGE = SHEET_NAME+'!H16:H1300' #Edit this to be just the cell H14
CAT_IMPORT_RANGE = SHEET_NAME+'!J17'
CONF_IMPORT_RANGE = SHEET_NAME+'!K17'


keys = google_funcs.decrypt_creds("./encrypt_google_cloud_credentials.json")
gsheet = google_funcs.gsheet_connect(keys)

df = google_funcs.gsheet_export(keys,spreadsheet_id,DESC_EXPORT_RANGE)

df =  df.convert_dtypes()
if df.empty:
    print("No descriptions added")
    exit()

dtypes = {col: 'str' for col in df.columns}

################################################################################
# MAKE PREDICTIONS #
################################################################################

vectorizer = pickle.load(open("vectorizer.pickle", "rb"))
model = pickle.load(open("model.pickle", "rb"))

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
