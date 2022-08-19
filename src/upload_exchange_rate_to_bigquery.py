from functions import google_funcs
import pandas as pd
import os
from forex_python.converter import CurrencyRates
from dotenv import load_dotenv
load_dotenv()

c = CurrencyRates()
from_date = pd.to_datetime('01/01/2020')
yesteday = pd.to_datetime("today") - pd.Timedelta(1, unit='D')
df = pd.DataFrame(pd.date_range(start=from_date,  end=yesteday, freq='MS'), columns=['date'])
def get_rate(x):
    try:
        op = c.get_rate('CAD', 'GBP', x)
    except Exception as re:
        print(re)
        op=None
    return op

df['cad_gbp_rate'] = df['date'].apply(get_rate)

# Convert Data types


keys = google_funcs.decrypt_creds("./encrypt_google_cloud_credentials.json")
google_funcs.gsheet_connect(keys)
client = google_funcs.big_query_connect(keys)

# Upload expenses
google_funcs.big_query_load_spending(
                    client,
                    table_id = "budgeting.exchange_rate_dim",
                    dataframe = df)
