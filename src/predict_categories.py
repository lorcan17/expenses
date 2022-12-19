from functions import google_funcs
import pandas as pd
import pickle
import nltk
from nltk.stem import PorterStemmer
import os
from dotenv import load_dotenv
load_dotenv()


################################################################################
# READ IN GSHEET COLUMN #
################################################################################

spreadsheet_id = os.environ['GSHEET_SHEET_ID']
gsheet_export_range = 'Splitwise Bulk Import!H14:H1300' #Edit this to be just the cell G14
gsheet_import_range = 'Splitwise Bulk Import!M15'
gsheet_clear_range = 'Expenses!A2:G10000'


keys = google_funcs.decrypt_creds("./encrypt_google_cloud_credentials.json")
gsheet = google_funcs.gsheet_connect(keys)

df = google_funcs.gsheet_export(keys,spreadsheet_id,gsheet_export_range)

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
descriptions = google_funcs.get_nlp_ready(descriptions)
descriptions = vectorizer.transform(descriptions)
df['categories'] = model.predict(descriptions)

data = [df.categories.values.tolist()]
value_range_body = {"values": data,
                    "majorDimension": "COLUMNS"}

gsheet.values().update(spreadsheetId=spreadsheet_id,
                            range=gsheet_import_range,
                            valueInputOption='RAW',
                            body =value_range_body
                            ).execute()
