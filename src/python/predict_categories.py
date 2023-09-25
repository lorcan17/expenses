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
DESC_EXPORT_RANGE = sheet_name+'!D17:E1300'  # Edit this to be just the cell H14
CAT_IMPORT_RANGE = sheet_name+'!G18'
CONF_IMPORT_RANGE = sheet_name+'!H18'

keys = google_funcs.decrypt_creds("./config/encrypt_google_cloud_credentials.json")
gsheet = google_funcs.gsheet_connect(keys)

df = google_funcs.gsheet_export(keys, spreadsheet_id, DESC_EXPORT_RANGE)

df = df.convert_dtypes()
df['Code'] = df['Code'].fillna('')
df['Description'] = df['Description'] + ' ' + df['Code']
df['Description'] = df['Description'].str.strip()
if df.empty:
    print("No descriptions added")
    exit()

################################################################################
# Fetch most used cat_name_subcat_name VALUES for each exp_desc#
################################################################################

# Build a list of unique descriptions to query
unique_descriptions = df['Description'].unique()

# Construct a comma-separated string of unique descriptions
unique_descriptions_str = ",".join([f'"{desc}"' for desc in unique_descriptions])

# Initialize a dictionary to store most recent cat_name_subcat_name values
most_recent_dict = {}

# Execute a single query to fetch most recent cat_name_subcat_name values for all descriptions
query = f"""
WITH RankedCategories AS (
    SELECT
        exp_desc,
        cat_name_subcat_name,
        ROW_NUMBER() OVER (PARTITION BY exp_desc ORDER BY COUNT(*) DESC) AS rn
    FROM
        budgeting.stg_expenses AS a
    INNER JOIN
        budgeting.dim_splitwise_category AS b
        ON a.subcat_id = b.subcat_id
    WHERE exp_desc IN ({unique_descriptions_str})
    AND date < '2023-08-01'
    GROUP BY
        exp_desc,
        cat_name_subcat_name
)
SELECT
    exp_desc,
    cat_name_subcat_name
FROM
    RankedCategories
WHERE rn = 1
"""
query_job = google_funcs.big_query_query(keys, query)

# Convert the query result to a DataFrame
result_df = query_job.to_dataframe()

# Iterate through the result DataFrame and store most recent values in the dictionary
for _, row in result_df.iterrows():
    most_recent_dict[row['exp_desc']] = row['cat_name_subcat_name']

# Map the most recent values to the original DataFrame based on Description
df['categories'] = df['Description'].map(most_recent_dict)
# Set 'conf' to 1 where 'categories' is not None
df['confidence'] = np.where(df['categories'].notnull(), 1, None)

################################################################################
# MAKE PREDICTIONS FOR None VALUES #
################################################################################

# Filter rows where most_recent is None
none_df = df[df['categories'].isnull()].copy()
print(len(none_df))
# Load the vectorizer and model
vectorizer = pickle.load(open("data/vectorizer.pickle", "rb"))
model = pickle.load(open("data/model.pickle", "rb"))

# Use the model to predict categories for None values
none_descriptions = none_df['Description']
none_descriptions = nlp_funcs.get_nlp_ready(none_descriptions)
none_descriptions = vectorizer.transform(none_descriptions)
none_df['categories'] = model.predict(none_descriptions)
proba = model.predict_proba(none_descriptions)
max_proba = np.amax(proba, axis=1)
none_df['confidence'] = max_proba

# Update the original DataFrame with predictions for None values
df.update(none_df)

################################################################################
# IMPORT RESULTS TO GSHEET #
################################################################################

# Import 'categories' and 'confidence' columns to Google Sheets
google_funcs.import_column(keys, df, 'categories', spreadsheet_id, CAT_IMPORT_RANGE)
google_funcs.import_column(keys, df, 'confidence', spreadsheet_id, CONF_IMPORT_RANGE)
