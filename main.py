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


# Create a function for the below

# In[7]:


# Clean Data
import random as r
import math

lorcan_paid = []
lorcan_owed = []
grace_paid = []
grace_owed = []

new_expenses_df = gsheets_export
for ind in new_expenses_df.index:
    cost = new_expenses_df['Cost'][ind]
    who_paid = new_expenses_df['Who Paid?'][ind]
    share = new_expenses_df['Share'][ind]
    if cost % 0.02 > 0:
        if r.random() >= 0.5:
            split_cost1 = math.floor((cost / 2) * 100) / 100
        else:
            split_cost1 = math.ceil((cost / 2)*100) / 100
    else:
        split_cost1 = cost / 2
    split_cost2 = cost - split_cost1
    if who_paid == 'Lorcan':
        lorcan_paid.append(cost)
        grace_paid.append(0)
    if who_paid == 'Grace':
        lorcan_paid.append(0)
        grace_paid.append(cost)
    if who_paid == 'Both':
        lorcan_paid.append(split_cost1)
        grace_paid.append(split_cost2)
    if share == 'Evenly':
        lorcan_owed.append(split_cost1)
        grace_owed.append(split_cost2)
    if share == 'Just Lorcan':
        lorcan_owed.append(cost)
        grace_owed.append(0)
    if share == 'Just Grace':
        lorcan_owed.append(0)
        grace_owed.append(cost)

new_expenses_df['Lorcan Paid'] = lorcan_paid
new_expenses_df['Lorcan Share'] = lorcan_owed
new_expenses_df['Grace Paid'] = grace_paid
new_expenses_df['Grace Share'] = grace_owed
#new_expenses_df
new_expenses_df = new_expenses_df.merge(cat_df, on ='Category', how = 'left')


# Create a function for the checks

# In[8]:


# Check Data
desc_ = []
id = []
error = []

total_errors = 0
df = new_expenses_df
for ind in df.index:
     no_error = 0
     date = df['Date'][ind]
     cost = df['Cost'][ind]
     desc = df['Description'][ind]
     lorcan_paid = df['Lorcan Paid'][ind]
     lorcan_owed = df['Lorcan Share'][ind]
     grace_paid = df['Grace Paid'][ind]
     grace_owed = df['Grace Share'][ind]

     if pd.isnull(date):
        no_error = 1 + no_error
        total_errors = 1 + total_errors
        desc_.append(desc)
        id.append(no_error)
        error.append(f"Error {no_error}: Date is not in the correct format:")


     if 'lorcan' in desc.lower() and lorcan_owed != cost:
        total_errors = 1 + total_errors
        no_error = 1 + no_error
        desc_.append(desc)
        id.append(no_error)
        print(f"Error {no_error}: Lorcan is found in description name but is split equally:")

     if 'grace' in desc.lower() and grace_owed != cost:
        total_errors = 1 + total_errors
        no_error = 1 + no_error
        desc_.append(desc)
        id.append(no_error)
        error.append(f"Error {no_error}: Grace is found in description name but is split equally:")


     if lorcan_owed == cost and not 'lorcan' in desc.lower():
        total_errors = 1 + total_errors
        no_error = 1 + no_error
        desc_.append(desc)
        id.append(no_error)
        error.append(f"Error {no_error}: Lorcan is not found in description name but has whole expense:")

     if grace_owed == cost and not 'grace' in desc.lower():
        total_errors = 1 + total_errors
        no_error = 1 + no_error
        desc_.append(desc)
        id.append(no_error)
        error.append(f"Error {no_error}: Grace is not found in description name but has whole expense:")

     if cost !=  lorcan_paid + grace_paid:
        total_errors = 1 + total_errors
        no_error = 1 + no_error
        desc_.append(desc)
        id.append(no_error)
        error.append(f"Error {no_error}: Paid amounts do not equal for:")

     if  cost != lorcan_owed + grace_owed:
        total_errors = 1 + total_errors
        no_error = 1 + no_error
        desc_.append(desc)
        id.append(no_error)
        error.append(f"Error {no_error}: Owed amounts do not equal for:")

df = pd.DataFrame(desc_, columns=['Description'])
df['ID'] = id

df['Error Message'] = error

if total_errors > 0:
    #print(f'There are {total_errors} that you need to fix')
    for ind in df.index:
        if df['ID'] [ind] == 1:
            logger.error(f"{df['Description'][ind]} (see below for error messages(s))")
        logger.info(f"{df['Error Message'] [ind]}")
    sys.exit("Review Log File for Errors")

else:
    logger.info("No Errors found")


# Create a function for the below

# In[9]:


# below loops through gsheet export and creates expense for each row
new_expenses_ids = []
expense_errors = []
expense_desc = []

df = new_expenses_df
for ind in df.index:
     #print(ind + 1)
     date = df['Date'][ind]
     desc = df['Description'][ind]
     #print(desc)
     cost = df['Cost'][ind]
     lorcan_paid = df['Lorcan Paid'][ind]
     lorcan_owed = df['Lorcan Share'][ind]
     grace_paid = df['Grace Paid'][ind]
     grace_owed = df['Grace Share'][ind]
     cat_id = df['CatID'][ind]
     expense = Expense()
     expense.setCost(cost)
     expense.setDescription(desc)
     expense.group_id = GroupId
     expense.date = date
     expense.category_id = cat_id
     user1 = ExpenseUser()
     user1.setId(LorcanId)
     user1.setPaidShare(lorcan_paid)
     user1.setOwedShare(lorcan_owed)
     user2 = ExpenseUser()
     user2.setId(GraceId)
     user2.setPaidShare(grace_paid)
     user2.setOwedShare(grace_owed) #grace_owed
     expense.addUser(user1)
     expense.addUser(user2)
     nExpense, errors = s.createExpense(expense)
     # check if there's an error
     try: #AttributeError
        error = errors.getErrors()
     except AttributeError:
        error = "No error"
     # print out expense_id
     expense_errors.append(error)
     try: #AttributeError
        expense_id = nExpense.getId()
     except AttributeError:
        expense_id = 0
     #print(nExpense.getId())
     #print(error)
     new_expenses_ids.append(expense_id)
     expense_desc.append(desc)
     if  error == "No error":
         logger.info(f'Expense created for {desc}')
     else:
        logger.error(f'Expense not created for {desc} - Error: {error}')

new_expense_ids_df  = pd.DataFrame(expense_desc, columns=['Description'])
new_expense_ids_df['ID'] = new_expenses_ids

new_expense_ids_df['Error Message'] = expense_errors

#new_expense_ids_df


# In[10]:


##print(expense_df)
##run the below if you need to delete all the expenses
def delete_expenses(df):
    rows = []
    rows2 = []
    for ind in df.index:
        #print(ind + 1,"ID: ",df["ID"][ind])
        success, errors = s.deleteExpense(df["ID"][ind])
        rows.append(success)
        try: #AttributeError
            error_message = errors.getErrors()
        except AttributeError:
            error_message = "No error"
        rows2.append(error_message)
        #print(error_message)

    df["Deletion Success"] = rows
    df["Deletion Error"] = rows2

# Check if there is an Error Message
expense_error_flag = 0
for ind in new_expense_ids_df.index:
    if new_expense_ids_df['Error Message'][ind] != 'No error':
        expense_error_flag = 1 + expense_error_flag




if expense_error_flag == 1:
    df = new_expense_ids_df
    df= df.loc[df['Error Message'] == "No error"]
    delete_expenses(df)
    logger.info("All Expenses deleted")
    #print("Please fix the below Expenses and try again:")
    df = new_expense_ids_df
    df= df.loc[df['Error Message'] != "No error"]
    raise
    for ind in df.index:
        True
        #print(df['Description'][ind])
        #print(df['Error Message'][ind])



# In[11]:


# Export Expenses
export = s.getExpenses(group_id = GroupId, limit=10000) #10000
rows = []
for e in export:
    cat = e.getCategory()
    subcat = cat.getSubcategories()
    category_name = cat.getName()
    users = e.getUsers()
    #if e.getDescription() == "UltraFit Bootcamp Lorcan":
    #    check = e
    lorcan_balance = 0
    grace_balance = 0
    for u in users:
        ID = u.getId()
        NetBalance = 0
        NetBalance = u.getNetBalance()
        if ID == LorcanId:
            lorcan_balance = NetBalance
        if ID == GraceId:
            grace_balance = NetBalance

    rows.append(
        [e.getDate(), e.getDescription(), category_name,e.getCost(),
         e.getCurrencyCode(), grace_balance ,lorcan_balance,e.getCreationMethod(),
         e.getDeletedAt()])




# In[12]:


# Clean Export
export_df = pd.DataFrame(rows, columns=["Date","Description","Category", "Cost",
                                        "Currency","Grace Williams", "Lorcan Travers",
                                        "creation_method","deleted_dt"])

export_df = export_df.loc[export_df['deleted_dt'].isnull()]
export_df = export_df.drop(['creation_method', 'deleted_dt'], axis=1)
#Format Date
export_df['Date'] = pd.to_datetime(export_df.Date)
export_df['Date'] = export_df['Date'].dt.strftime('%d/%m/%Y')


# In[13]:


# Delete current data

empty_rows = []

for i in range(10000 - 1):
    thislist = []
    for j  in range(7):
        thislist.insert(1,"")

    empty_rows.append(thislist)

request = gsheet.values().update(spreadsheetId=spreadsheet_id, range= gsheet_clear_range,
                                 valueInputOption="USER_ENTERED", body={"values":empty_rows}).execute()
logger.info("GSheet data removed succesfully")


# In[14]:


#Enter new data

df_list = export_df.values.tolist()
request = gsheet.values().update(spreadsheetId=spreadsheet_id, range= gsheet_import_range,
                                 valueInputOption="USER_ENTERED", body={"values":df_list}).execute()
logger.info("Data imported succesfully")


# In[15]:


if test:
    delete_expenses(new_expense_ids_df)
    logger.info('All expenses deleted as this was a test run')


# In[ ]:
