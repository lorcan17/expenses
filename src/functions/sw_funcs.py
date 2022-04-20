#from splitwise import Splitwise
import os
from splitwise import Splitwise
from dotenv import load_dotenv
load_dotenv()
import pandas as pd
#from datetime import datetime as dt

def sw_connect_api():
    s_consumer_key = os.environ['SPLITWISE_CONSUMER_KEY']
    s_consumer_secret = os.environ['SPLITWISE_CONSUMER_SECRET_KEY']
    s_api_key = os.environ['SPLITWISE_API_KEY']

    s = Splitwise(s_consumer_key,
                  s_consumer_secret,
                  api_key = s_api_key)
    return s

def sw_current_user(s):
    u = s.getCurrentUser()
    id = u.getId()
    return id

def sw_other_user(s, first_name, last_name):
    friends = s.getFriends()
    for x in friends:
      if x.getFirstName().lower() == first_name.lower() \
      and x.getLastName().lower() == last_name.lower():
          id = x.getId()
          return id

def sw_group_id(s, group_name):
    groups = s.getGroups()
    for x in groups:
      if x.getName().lower() == group_name.lower():
          id = x.getId()
          return id

def sw_export_data(s,group_id,limit = 100000):
    export = s.getExpenses(group_id = group_id, limit=limit) #10000
    rows = []
    for e in export:
        #date
        date = e.getDate()
        deleted_date = e.getDeletedAt()
        #expense info
        exp_id = e.getId()
        cat = e.getCategory()
        subcat = cat.getSubcategories()
        category_name = cat.getName()
        exp_desc = e.getDescription()
        # Other
        creation_method = e.getCreationMethod()
        # Cost
        exp_cost = e.getCost()
        exp_currency = e.getCurrencyCode()
        # User info
        users = e.getUsers()
        for u in users:
            user_id = u.getId()
            first_name = u.getFirstName()
            last_name = u.getLastName()
            net_balance = u.getNetBalance()
            paid_share = u.getPaidShare()
            owed_share = u.getOwedShare()

            rows.append(
                [date, deleted_date, exp_id, category_name,exp_desc,
                creation_method, exp_cost ,exp_currency,user_id,
                 first_name,last_name,net_balance,paid_share,owed_share])







    # In[12]:


    # Clean Export
    export_df = pd.DataFrame(rows,
    columns=["date", "deleted_date", "exp_id", "category_name","exp_desc",
    "creation_method", "exp_cost" ,"exp_currency","user_id",
     "first_name","last_name","net_balance","paid_share","owed_share"])

    df_dtypes = {
    "date" : "datetime64[ns]",
    "deleted_date" : "datetime64[ns]" ,
    "exp_id" : "int64",
    "category_name" : "category",
    "exp_desc" :  "object",
    "creation_method" : "object",
    "exp_cost" : "float",
    "exp_currency" : "category",
    "user_id" : "int64",
     "first_name" : "category",
     "last_name" : "category",
     "net_balance" : "float",
     "paid_share": "float",
     "owed_share" : "float"
     }
    export_df = export_df.astype(df_dtypes)

    #export_df = export_df.loc[export_df['deleted_dt'].isnull()]
    #export_df = export_df.drop(['creation_method', 'deleted_dt'], axis=1)
    #Format Date
    #export_df['date'] = export_df['date'].dt.date()
    #export_df['date'] = export_df['date'].dt.strftime('%d/%m/%Y')
    #export_df['deleted_date'] = pd.to_datetime(export_df.date)
    #export_df['deleted_date'] = export_df['deleted_date'].dt.strftime('%d/%m/%Y')
    return export_df
