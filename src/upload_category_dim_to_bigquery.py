from dotenv import load_dotenv
from functions import google_funcs, sw_funcs
load_dotenv()

s = sw_funcs.sw_connect_api()
df = sw_funcs.sw_get_category_dim(s)
df = df.rename(columns = {"cat_name: subcat_name": "cat_name_subcat_name"})

# Convert Data types

keys = google_funcs.decrypt_creds("./encrypt_google_cloud_credentials.json")

client = google_funcs.big_query_connect(keys)

# Upload expenses
google_funcs.big_query_load_spending(
                    client,
                    table_id = "budgeting.dim_splitwise_category",
                    dataframe = df)
