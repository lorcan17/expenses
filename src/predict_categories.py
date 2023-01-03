import os
import pickle

import nltk
from dotenv import load_dotenv
from functions import google_funcs

nltk.download('punkt')
load_dotenv()


################################################################################
# READ IN GSHEET COLUMN #
################################################################################

spreadsheet_id = os.environ['GSHEET_SHEET_ID']
GSHEET_EXPORT_RANGE = 'Splitwise Bulk Import!H14:H1300' #Edit this to be just the cell G14
GSHEET_IMPORT_RANGE = 'Splitwise Bulk Import!M15'


keys = google_funcs.decrypt_creds("./encrypt_google_cloud_credentials.json")
gsheet = google_funcs.gsheet_connect(keys)

df = google_funcs.gsheet_export(keys,spreadsheet_id,GSHEET_EXPORT_RANGE)

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
                            range=GSHEET_IMPORT_RANGE,
                            valueInputOption='RAW',
                            body =value_range_body
                            ).execute()
