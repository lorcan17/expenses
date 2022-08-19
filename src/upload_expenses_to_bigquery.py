from functions import sw_funcs, google_funcs
import pandas as pd

s = sw_funcs.sw_connect_api()
group_id = sw_funcs.sw_group_id(s,"Everyday spEnding")


dates = pd.DataFrame(
            {'date_from':pd.date_range(
                    start='1/1/2020', end = pd.to_datetime('today'),freq='YS'),
            'date_to':pd.date_range(
                    start='1/1/2020', end = pd.to_datetime('today') + pd.to_timedelta(365,'D'),freq='Y')
            })

for ind in dates.index:
    date_from = dates['date_from'][ind]
    date_to = dates['date_to'][ind]
    export = sw_funcs.sw_export_data(s,group_id,limit = 0, date_before = date_to, date_after = date_from )
    if ind == 0:
        write_disposition = 'WRITE_TRUNCATE'
    else:
        write_disposition = 'WRITE_APPEND'

    keys = google_funcs.decrypt_creds("./encrypt_google_cloud_credentials.json")
    google_funcs.gsheet_connect(keys)
    client = google_funcs.big_query_connect(keys)

    # Upload expenses
    google_funcs.big_query_load_spending(
                        client,
                        table_id = "budgeting.splitwise_expenses",
                        dataframe = export,
                        write_disposition = write_disposition)
