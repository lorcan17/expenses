#from splitwise import Splitwise
import os
import pandas as pd # pylint: disable=import-error
from splitwise import Splitwise # pylint: disable=import-error
from dotenv import load_dotenv
load_dotenv()
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
    user = s.getCurrentUser()
    id = user.getId()
    return id

def sw_other_user(s, first_name, last_name):
    friends = s.getFriends()
    for user in friends:
        if user.getFirstName().lower() == first_name.lower() \
        and user.getLastName().lower() == last_name.lower():
            id = user.getId()
            return id

def sw_group_id(s, group_name):
    groups = s.getGroups()
    for x in groups:
        if x.getName().lower() == group_name.lower():
            id = x.getId()
            return id

def sw_get_category_dim(s):
    """Create a Dataframe with all Splitwise Categories, with IDs"""
    rows = []

    cat = s.getCategories()

    for x in cat:
        subcat =  x.getSubcategories()
        subcat_id = x.getId()
        cat_name = x.getName()
        cat_id = x.getId()
        for y in subcat:
            subcat_name = y.getName()
            subcat_id = y.getId()
            rows.append([cat_id, cat_name,subcat_id, subcat_name])

    cat_df = pd.DataFrame(rows, columns=
    ["cat_id", "cat_name","subcat_id","subcat_name"])
    cat_df['cat_name: subcat_name'] = cat_df['cat_name'] + ': ' +  cat_df['subcat_name']

    df_dtypes = {
    "cat_id" : "int64",
    "cat_name" : "category" ,
    "subcat_id" : "int64",
    "subcat_name" : "category",
    "cat_name: subcat_name" : "category"
    }
    #cat_df['Category'] = cat_df['Main Category'] + ': ' +  cat_df['SubCategory']
    cat_df = cat_df.astype(df_dtypes)
    return cat_df

def sw_export_data(s,group_id,date_after,date_before,limit = 0):
    export = s.getExpenses(
        group_id = group_id, limit=limit, dated_before = date_before,
        dated_after = date_after
        ) #10000
    rows = []
    for exp in export:
        #date
        date = exp.getDate()
        deleted_date = exp.getDeletedAt()
        created_date = exp.getCreatedAt()
        updated_date = exp.getUpdatedAt()
        #expense info
        exp_id = exp.getId()
        cat = exp.getCategory()
        subcat_id = cat.getId()
        subcat_name = cat.getName()
        exp_desc = exp.getDescription()
        # Other
        creation_method = exp.getCreationMethod()
        # Cost
        exp_cost = exp.getCost()
        exp_currency = exp.getCurrencyCode()
        # User info
        users = exp.getUsers()
        for user in users:
            user_id = user.getId()
            first_name = user.getFirstName()
            last_name = user.getLastName()
            net_balance = user.getNetBalance()
            paid_share = user.getPaidShare()
            owed_share = user.getOwedShare()

            rows.append(
                [date, deleted_date, created_date, updated_date, exp_id,
                subcat_id,subcat_name,exp_desc, creation_method, exp_cost,
                exp_currency,user_id, first_name,last_name,net_balance,paid_share,
                owed_share])

    # Clean Export
    export_df = pd.DataFrame(rows,
    columns=["date", "deleted_date", "created_date","updated_date", "exp_id",
    "subcat_id", "subcat_name","exp_desc", "creation_method", "exp_cost",
    "exp_currency","user_id", "first_name","last_name","net_balance",
    "paid_share", "owed_share"])

    df_dtypes = {
    "date" : "datetime64[ns]",
    "deleted_date" : "datetime64[ns]" ,
    "created_date" : "datetime64[ns]",
    "updated_date" : "datetime64[ns]" ,
    "exp_id" : "int64",
    "subcat_id" : "int64",
    "subcat_name" : "category",
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
    #TODO remove subcat_name as this can be derived from cat_dim table
    return export_df
