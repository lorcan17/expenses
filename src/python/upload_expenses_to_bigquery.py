import os 
import pandas as pd
from functions import google_funcs, sw_funcs

s = sw_funcs.sw_connect_api()

keys = google_funcs.decrypt_creds("./config/encrypt_google_cloud_credentials.json")
client = google_funcs.big_query_connect(keys)

splitwise_group = 'Everyday Spending'
print(f'Pulling from {splitwise_group}')
group_id = sw_funcs.sw_group_id(s,splitwise_group)

# Get last updated date
last_updated = google_funcs.big_query_export(keys,
             '''SELECT DATE(MAX(updated_dt)) as date FROM python-splitwise.budgeting.t_expenses_stage''') 


date_from = last_updated["date"][0] - pd.to_timedelta(7,'D')
date_to = pd.to_datetime('today') + pd.to_timedelta(1,'D')
export = sw_funcs.sw_export_data_v2(
    s,group_id,limit = 0,
    updated_before = date_to,
    updated_after = date_from
    )

# Upload expenses
google_funcs.big_query_load_spending(
                    client,
                    table_id = "budgeting.t_expenses_stage",
                    dataframe = export,
                    write_disposition = 'WRITE_TRUNCATE')

# Merge into Fact
google_funcs.big_query_query(keys, 'src/sql/dml/expenses_merge.sql', True)
google_funcs.big_query_query(keys, "delete budgeting.t_expenses_stage WHERE true")                