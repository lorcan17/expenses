#! /usr/bin/env python

import os

import pandas as pd 
from dotenv import load_dotenv

from splitwise.expense import Expense
from splitwise.expense import ExpenseUser

from functions import google_funcs, sw_funcs
load_dotenv()

spreadsheet_id = os.environ['GSHEET_SHEET_ID']
GSHEET_EXPORT_RANGE = 'Splitwise Bulk Import!G14:01300'
GSHEET_IMPORT_RANGE = 'Expenses!A2'

s = sw_funcs.sw_connect_api()

group_id = sw_funcs.sw_group_id(s,"Everyday spEnding")
LorcanId = sw_funcs.sw_current_user(s)
GraceId = sw_funcs.sw_other_user(s,"Grace", "Williams")

cat_dim = sw_funcs.sw_get_category_dim(s)

keys = google_funcs.decrypt_creds("./encrypt_google_cloud_credentials.json")

df = google_funcs.gsheet_export(keys,spreadsheet_id,GSHEET_EXPORT_RANGE)
# Convert Data types
df =  df.convert_dtypes()

if df.empty:
    print("No expenses to upload to SplitWise")
    exit()

df['Date'] = pd.to_datetime(df['Date'] ,errors = 'coerce',format = '%Y%m%d')
df['Cost'] = df['Cost'].str.replace(',', '')
df['Cost'] = pd.to_numeric(df['Cost'])
df['Description'] = df['Description'].str.title()
# Add 50-50 where Share = "Split" and split is empty
df.loc[(df["Share"]=="Split") & (df["Split"].isna()),"Split"] = "50-50"

if not df["Split"].isna().all():
    df['split_payer'], df['split_nonpayer'] = df['Split'].str.split('-',expand=True)
    df['split_payer'] = pd.to_numeric(df['split_payer'])
    df['split_nonpayer'] = pd.to_numeric(df['split_nonpayer'])
else: # No items are split and below is to avoid error
    df['split_payer'] = 0
    df['split_nonpayer'] = 0



# Sort Expenses
lorcan_paid = []
lorcan_owed = []
grace_paid = []
grace_owed = []

new_expenses_df = df
for ind in new_expenses_df.index:
    cost = new_expenses_df['Cost'][ind]
    who_paid = new_expenses_df['Who Paid?'][ind]
    share = new_expenses_df['Share'][ind]
    split_payer = new_expenses_df['split_payer'][ind]
    split_nonpayer = new_expenses_df['split_nonpayer'][ind]
    payer_cost = cost * (split_payer/100)
    payer_cost = round(payer_cost,2)
    nonpayer_cost = cost - payer_cost

    if who_paid == 'Lorcan':
        lorcan_paid.append(cost)
        grace_paid.append(0)
    if who_paid == 'Grace':
        lorcan_paid.append(0)
        grace_paid.append(cost)
    if who_paid == 'Both':
        lorcan_paid.append(payer_cost)
        grace_paid.append(nonpayer_cost)
    if share == 'Split':
        if who_paid == 'Lorcan':
            lorcan_owed.append(payer_cost)
            grace_owed.append(nonpayer_cost)
        if who_paid == 'Grace':
            grace_owed.append(payer_cost)
            lorcan_owed.append(nonpayer_cost)
        if who_paid == 'Both':
            lorcan_owed.append(payer_cost)
            grace_owed.append(nonpayer_cost)
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

new_expenses_df = new_expenses_df.merge(
                                    cat_dim, left_on ='Category',
                                    right_on = 'cat_name: subcat_name', how = 'left')
#print(new_expenses_df)



new_expenses_ids = []
expense_errors = []
expense_desc = []

df = new_expenses_df
#print(df.columns)
for ind in df.index:
    date = df['Date'][ind]
    #print(ind + 1)
    desc = df['Description'][ind]
    #print(desc)
    cost = df['Cost'][ind]
    lorcan_paid = df['Lorcan Paid'][ind]
    lorcan_owed = df['Lorcan Share'][ind]
    grace_paid = df['Grace Paid'][ind]
    grace_owed = df['Grace Share'][ind]
    cat_id = df['subcat_id'][ind]
    currency = df['Currency'][ind]
    expense = Expense()
    expense.setCost(cost)
    expense.setDescription(desc)
    expense.setCurrencyCode(currency)
    expense.group_id = group_id
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
    try:
        error = errors.getErrors()
    except AttributeError:
        error = "No error"
    expense_errors.append(error)
    try:
        expense_id = nExpense.getId()
    except AttributeError:
        expense_id = 0
    new_expenses_ids.append(expense_id)
    expense_desc.append(desc)
    if  error == "No error":
        print(f'Expense {expense_id} created')
    else:
        print(f'Error: {error}')

new_expense_ids_df  = pd.DataFrame(expense_desc, columns=['Description'])
new_expense_ids_df['ID'] = new_expenses_ids
#new_expense_ids_df.to_csv("new_ids.csv")
