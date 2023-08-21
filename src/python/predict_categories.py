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
DESC_EXPORT_RANGE = sheet_name+'!D17:E1300' #Edit this to be just the cell H14
CAT_IMPORT_RANGE = sheet_name+'!G18'
CONF_IMPORT_RANGE = sheet_name+'!H18'

keys = google_funcs.decrypt_creds("./config/encrypt_google_cloud_credentials.json")
gsheet = google_funcs.gsheet_connect(keys)

df = google_funcs.gsheet_export(keys,spreadsheet_id,DESC_EXPORT_RANGE)

df =  df.convert_dtypes()
df['Code'] = df['Code'].fillna('')
df['Description'] = df['Description'] + ' ' + df['Code']
df['Description'] = df['Description'].str.strip()
if df.empty:
    print("No descriptions added")
    exit()

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

google_funcs.import_column(keys, df,'categories',spreadsheet_id, CAT_IMPORT_RANGE)

google_funcs.import_column(keys, df,'confidence',spreadsheet_id, CONF_IMPORT_RANGE)
