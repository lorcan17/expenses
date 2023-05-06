import os 
import pandas as pd
from functions import google_funcs, sw_funcs

s = sw_funcs.sw_connect_api()

test_run = os.environ['TEST_RUN']
GROUP_NAME = "Everyday spEnding" if test_run == "No" else "Test"
group_id = sw_funcs.sw_group_id(s,GROUP_NAME)


dates = pd.DataFrame(
    {'date_from':
        pd.date_range(
        start='1/1/2020',
        end = pd.to_datetime('today'),
        freq='Q'
        )
date_to_list = pd.date_range(
        start='2020-04-01',
        end = pd.to_datetime('today') + pd.to_timedelta(90,'D'),
        freq='QS')

dates = pd.DataFrame(
    {
        'date_from': date_from_list,
        'date_to': date_to_list
    }
    )
print(dates)
for ind in dates.index:
    date_from = dates['date_from'][ind]
    date_to = dates['date_to'][ind]
    export = sw_funcs.sw_export_data(
        s,group_id,limit = 0,
        date_before = date_to,
        date_after = date_from
        )
    if ind == 0:
        WRITE_DISPOSITION = 'WRITE_TRUNCATE'
    else:
        WRITE_DISPOSITION = 'WRITE_APPEND'

    keys = google_funcs.decrypt_creds("./encrypt_google_cloud_credentials.json")
    google_funcs.gsheet_connect(keys)
    client = google_funcs.big_query_connect(keys)

    # Upload expenses
    google_funcs.big_query_load_spending(
                        client,
                        table_id = "budgeting.splitwise_expenses",
                        dataframe = export,
                        write_disposition = WRITE_DISPOSITION)
