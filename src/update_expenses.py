from splitwise.expense import Expense
from splitwise.expense import ExpenseUser
import os
from dotenv import load_dotenv
from functions import sw_funcs, google_funcs
load_dotenv()

spreadsheet_id = os.environ['GSHEET_SHEET_ID']
gsheet_export_range = 'Update Expenses!A1:R1300'

s = sw_funcs.sw_connect_api()

cat_dim = sw_funcs.sw_get_category_dim(s)

keys = google_funcs.decrypt_creds("./encrypt_google_cloud_credentials.json")

df = google_funcs.gsheet_export(keys,spreadsheet_id,gsheet_export_range)

if df.empty:
    print("No expenses to be updated")
    exit()

df = df.merge(cat_dim, left_on ='new_cat', right_on = 'cat_name: subcat_name', how = 'left')

s = sw_funcs.sw_connect_api()
LorcanId = sw_funcs.sw_current_user(s)
GraceId = sw_funcs.sw_other_user(s,"Grace", "Williams")

for ind in df.index:
    id = df["exp_id"][ind]
    delete = df['delete (yes/no)'][ind]
    new_desc = df['new_desc'][ind]
    new_cost = df['new_cost'][ind]
    new_lorcan_paid = df['new_lorcan_paid'][ind]
    new_lorcan_owed = df['new_lorcan_owed'][ind]
    new_grace_paid = df['new_grace_paid'][ind]
    new_grace_owed = df['new_grace_owed'][ind]
    new_cat = df['new_cat'][ind]
    new_subcat_id = df['subcat_id'][ind]

    if delete == 'yes':
        s.deleteExpense(id)
        print(f'Expense {id} deleted')
        continue

    expense = Expense()
    expense.id = id

    if new_desc:
        expense.setDescription(new_desc)

    if new_cat:
        expense.category_id = new_subcat_id

    if new_cost:
        expense.cost = new_cost
        user1 = ExpenseUser()
        user1.setId(LorcanId)
        user1.setPaidShare(new_lorcan_paid)
        user1.setOwedShare(new_lorcan_owed)
        user2 = ExpenseUser()
        user2.setId(GraceId)
        user2.setPaidShare(new_grace_paid)
        user2.setOwedShare(new_grace_owed) #grace_owed
        expense.addUser(user1)
        expense.addUser(user2)

    if new_desc or new_cat or new_cost:
        s.updateExpense(expense)
        print(f'Expense {id} updated')
