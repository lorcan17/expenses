import os
import pandas as pd
import re
from dotenv import load_dotenv
from functions import google_funcs

load_dotenv()


def preprocess_text(text):
    # Replace special characters with spaces
    cleaned_text = re.sub(r'/', ' ', text)
    # Trim leading and trailing spaces
    trimmed_text = cleaned_text.strip()
    return trimmed_text

def replace_desc(row):
    if pd.isnull(row['Replace With']):
        return row['Description Contains']
    else:
        return row['Replace With']

################################################################################
# READ IN GSHEET COLUMN #
################################################################################

spreadsheet_id = os.environ['GSHEET_SHEET_ID']
keys = google_funcs.decrypt_creds("./config/encrypt_google_cloud_credentials.json")

# Cleaning Sheet
sheet_name = 'Cleaning'
clean_1_export = sheet_name+'!A5:B1300' #Edit this to be just the cell H14
clean_2_export = sheet_name+'!D5:D1300'
clean_3_export = sheet_name+'!F5:F1300'

clean_1_df = google_funcs.gsheet_export(keys,spreadsheet_id,clean_1_export)
clean_1_df['Replace With'] = clean_1_df.apply(replace_desc, axis=1)

clean_2_df = google_funcs.gsheet_export(keys,spreadsheet_id,clean_2_export)

clean_3_df = google_funcs.gsheet_export(keys,spreadsheet_id,clean_3_export)

# Expenses Sheet
sheet_name = 'Expenses'
DESC_EXPORT_RANGE = sheet_name+'!B17:B1300' #Edit this to be just the cell H14
NEW_DESC_IMPORT_RANGE = sheet_name+'!D18'

df = google_funcs.gsheet_export(keys,spreadsheet_id,DESC_EXPORT_RANGE)

if df.empty:
    print("No descriptions added")
    exit()

df['Description'] = df['Bank Description']
df['Description'] = df['Description'].fillna('')
# Loop through and replace descriptions
for index, row in clean_1_df.iterrows():
    condition = df['Description'].str.contains(row['Description Contains'], case=False)
    condition = condition.fillna(False)  # Replace NA/NaN values with False
    df.loc[condition, 'Description'] = row['Replace With']

# Loop through remove strings from df1
for remove_str in clean_2_df['Remove From Description']:
    df['Description'] = df['Description'].str.replace(remove_str, '', case=False, regex=False)

df =  df.convert_dtypes()
df['Description'] = df['Description'].apply(preprocess_text)
df['Description'] = df['Description'].str.strip()
df['Description'] = df['Description'].str.replace(r'\s+', ' ', regex=True).str.strip()
df['Description'] = df['Description'].str.lower()
df['Description'] = df['Description'].str.title()
df['Description'] = df['Description'].str.replace("'S", "'s", regex=False)

# Iterate through each word in the 'Capatilise' column of clean_3_df
for word in clean_3_df['Capatilise']:
    # Create a condition to find matching words in 'Description' column (case-insensitive)
    condition = df['Description'].str.contains(rf'\b{word}\b', case=False, regex=True)
    # Apply the capitalization to matching words
    df.loc[condition, 'Description'] = df.loc[condition, 'Description'].str.replace(
        rf'\b{word}\b', word.upper(), regex=True, case = False)



################################################################################
# IMPORT NEW DESCRIPTIONS #
################################################################################


google_funcs.import_column(keys, df,'Description',spreadsheet_id, NEW_DESC_IMPORT_RANGE)
