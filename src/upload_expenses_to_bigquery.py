from functions import sw_funcs, google_funcs
import pandas as pd

s = sw_funcs.sw_connect_api()
group_id = sw_funcs.sw_group_id(s,"Everyday spEnding")
#cat_dim = sw_funcs.sw_get_category_dim(s)
export = sw_funcs.sw_export_data(s,group_id,limit = 100000)

keys = google_funcs.decrypt_creds("./encrypt_google_cloud_credentials.json")
google_funcs.gsheet_connect(keys)
client = google_funcs.big_query_connect(keys)

# Upload expenses
google_funcs.big_query_load_spending(
                    client,
                    table_id = "budgeting.splitwise_expenses",
                    dataframe = export)
