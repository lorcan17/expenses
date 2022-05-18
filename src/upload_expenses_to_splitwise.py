from functions import sw_funcs, google_funcs
import pandas as pd
from splitwise.expense import Expense
from splitwise.expense import ExpenseUser

spreadsheet_id = '1CpbYfhi6bbXz5oqMs6mJs4y5ETlq2-mSCkLaU_9Wo68'
gsheet_export_range = 'Splitwise Bulk Import!G14:L1300' #Edit this to be just the cell G14
gsheet_import_range = 'Expenses!A2'
gsheet_clear_range = 'Expenses!A2:G10000'

s = sw_funcs.sw_connect_api()

group_id = sw_funcs.sw_group_id(s,"Everyday spEnding")
LorcanId = sw_funcs.sw_current_user(s)
GraceId = sw_funcs.sw_other_user(s,"Grace", "Williams")

cat_dim = sw_funcs.sw_get_category_dim(s)

keys = google_funcs.decrypt_creds("./encrypt_google_cloud_credentials.json")
gsheet = google_funcs.gsheet_connect(keys)

result = gsheet.values().get(spreadsheetId=spreadsheet_id,
                            range=gsheet_export_range).execute()
values = result.get('values', [])
    # Format as DF and promote first row as headers
df = pd.DataFrame(values)
header_row = 0
df.columns = df.iloc[header_row]
df = df.drop(header_row)
df = df.reset_index(drop=True)

# Convert Data types
df =  df.convert_dtypes()
df['Date'] = pd.to_datetime(df['Date'] ,errors = 'coerce',format = '%Y%m%d')
df['Cost'] = pd.to_numeric(df['Cost'])
#print(gsheets_export)
#df = google_funcs.gsheet_export(gsheet,spreadsheet_id,gsheet_export_range,date_format = '%Y%m%d')
#print(df)
import random as r
import math

lorcan_paid = []
lorcan_owed = []
grace_paid = []
grace_owed = []

new_expenses_df = df
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

new_expenses_df = new_expenses_df.merge(cat_dim, left_on ='Category', right_on = 'cat_name: subcat_name', how = 'left')
#print(new_expenses_df)



new_expenses_ids = []
expense_errors = []
expense_desc = []

df = new_expenses_df
#print(df.columns)
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
     cat_id = df['subcat_id'][ind]
     expense = Expense()
     expense.setCost(cost)
     expense.setDescription(desc)
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
         print(f'Expense created for {desc}')
     else:
        print(f'Expense not created for {desc} - Error: {error}')

new_expense_ids_df  = pd.DataFrame(expense_desc, columns=['Description'])
new_expense_ids_df['ID'] = new_expenses_ids
new_expense_ids_df.to_csv("new_ids.csv")
#google_funcs.big_query_load_spending(
#                    client,
#                    table_id = "budgeting.dim_splitwise_category",
#                    dataframe = cat_dim)
