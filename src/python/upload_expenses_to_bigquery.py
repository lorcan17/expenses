import os 
import pandas as pd
from functions import google_funcs, sw_funcs

s = sw_funcs.sw_connect_api()

splitwise_group = 'Everyday expenses'
group_id = sw_funcs.sw_group_id(s,splitwise_group)
date_from = pd.to_datetime('today') - pd.to_timedelta(190,'D')
date_to = pd.to_datetime('today') + pd.to_timedelta(1,'D')
export = sw_funcs.sw_export_data_v2(
    s,group_id,limit = 0,
    updated_before = date_to,
    updated_after = date_from
    )
keys = google_funcs.decrypt_creds("./config/encrypt_google_cloud_credentials.json")
# Got to be worried about loading test data into bigquery
google_funcs.gsheet_connect(keys)
client = google_funcs.big_query_connect(keys)
# Upload expenses
google_funcs.big_query_load_spending(
                    client,
                    table_id = "budgeting.t_expenses_stage",
                    dataframe = export,
                    write_disposition = 'WRITE_TRUNCATE')

# Merge into Fact
google_funcs.big_query_query(keys, 'src/sql/dml/expenses_merge.sql', True)
google_funcs.big_query_query(keys, "delete budgeting.t_expenses_stage WHERE true")                