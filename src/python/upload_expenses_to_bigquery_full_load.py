import pandas as pd
from functions import google_funcs, sw_funcs

s = sw_funcs.sw_connect_api()


splitwise_group = 'Everyday Spending'
print(f'Pulling from {splitwise_group}')
group_id = sw_funcs.sw_group_id(s,splitwise_group)

date_from_list = pd.date_range(
        start='2019-12-01',
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
    print(f'Pushing data from {date_from} to {date_to}')
    export = sw_funcs.sw_export_data(
        s,group_id,limit = 0,
        date_before = date_to,
        date_after = date_from
        )
    if ind == 0:
        WRITE_DISPOSITION = 'WRITE_TRUNCATE'
    else:
        WRITE_DISPOSITION = 'WRITE_APPEND'

    keys = google_funcs.decrypt_creds("./config/encrypt_google_cloud_credentials.json")
    google_funcs.gsheet_connect(keys)
    client = google_funcs.big_query_connect(keys)

    # Upload expenses
    google_funcs.big_query_load_spending(
                        client,
                        table_id = "budgeting.t_expenses_stage",
                        dataframe = export,
                        write_disposition = WRITE_DISPOSITION)

# Merge into Fact
google_funcs.big_query_query(keys, 'src/sql/dml/expenses_merge.sql', True)
google_funcs.big_query_query(keys, "delete budgeting.t_expenses_stage WHERE true")                
