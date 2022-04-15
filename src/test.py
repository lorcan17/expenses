#!/usr/bin/env python
# coding: utf-8

# # Splitwise Bulk Importer
#
# The following script is set up in following way
# - connect to google sheet
# - export transaction data from a google sheet  _spending_ tab name _Splitwise Bulk Import_
# - Connect to SplitWise API
# - Import transaction data into Splitise group _everyday spending_
# - Export all transactions from Splitise group _everyday spending_
# - Import all transaction to google sheet _spending_ tab nam _Expenses_
#

# In[1]:

import os
from dotenv import load_dotenv
import pandas as pd
import sys
from datetime import datetime as dt
from splitwise import Splitwise
from splitwise.expense import Expense
from splitwise.expense import ExpenseUser
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
from GetCategoryIDs import GetCategoryIDs
import logging
from cryptography.fernet import Fernet

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler = logging.FileHandler('splitwise.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


test = False #True


# In[2]:


# Connect to Splitwise
s_consumer_key = os.environ['SPLITWISE_CONSUMER_KEY']
s_consumer_secret = os.environ['SPLITWISE_CONSUMER_SECRET_KEY']
s_api_key = os.environ['SPLITWISE_API_KEY']

s = Splitwise(s_consumer_key,
              s_consumer_secret,
              api_key = s_api_key)


# In[3]:


# Connect to GSheets
ENCRYPT_KEY= os.environ['ENCRYPT_KEY']
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
#keys = ast.literal_eval(GOOGLE_JSON_KEY)
f = Fernet(ENCRYPT_KEY)
with open("encrypt_google_cloud_credentials.json", "rb") as file:
# read the encrypted data
    encrypted_data = file.read()
# decrypt data
    keys = f.decrypt(encrypted_data)
    #(keys)
    import json
keys = json.loads(keys)
    #keys = ast.literal_eval(keys)
creds = None
#creds = service_account.Credentials.from_service_account_file(
#        keys, scopes=SCOPES)
creds = service_account.Credentials.from_service_account_info(
        keys, scopes=SCOPES)


# The ID and ranges required of spreadsheet.
spreadsheet_id = '1CpbYfhi6bbXz5oqMs6mJs4y5ETlq2-mSCkLaU_9Wo68'
gsheet_export_range = 'Splitwise Bulk Import!G14:L1300' #Edit this to be just the cell G14
gsheet_import_range = 'Expenses!A2'
gsheet_clear_range = 'Expenses!A2:G10000' #gsheet_import_range+':G10000'#Remove this and make function dynamic mased on import cell


service = build('sheets', 'v4', credentials=creds)

# Call the Sheets API
gsheet = service.spreadsheets()


# In[4]:


# Get Lorcan ID
u = s.getCurrentUser()
LorcanId = u.getId()

# Get Grace ID
friends = s.getFriends()
for x in friends:
  if x.getFirstName() == "Grace" and x.getLastName() == "Williams":
      GraceId = x.getId()

#Get Group ID
groups = s.getGroups()
for x in groups:
  if x.getName() == "Everyday spending":
      GroupId = x.getId()


# In[5]:


cat_df = GetCategoryIDs(s)


# In[6]:


# export transaction data from google sheet
result = gsheet.values().get(spreadsheetId=spreadsheet_id,
                            range=gsheet_export_range).execute()
values = result.get('values', [])

# Format as DF and promote first row as headers
gsheets_export = pd.DataFrame(values)
header_row = 0
gsheets_export.columns = gsheets_export.iloc[header_row]
gsheets_export = gsheets_export.drop(header_row)
gsheets_export = gsheets_export.reset_index(drop=True)

# Convert Data types
gsheets_export =  gsheets_export.convert_dtypes()
gsheets_export['Date'] = pd.to_datetime(gsheets_export['Date'] ,errors = 'coerce',format = '%Y%m%d')
gsheets_export['Cost'] = pd.to_numeric(gsheets_export['Cost'])
print(gsheets_export)
