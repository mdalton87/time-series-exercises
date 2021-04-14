import requests
import pandas as pd
from io import StringIO






def _create_df_from_payloads(endpoint, max_pages, target_key_name):
    """
    Helper function that loops through the pages returned from the Zach API,\
    adds the information to a list and then converts to a single dataframe that is returned.
    """
    page_list = []
    for i in range(1, max_pages + 1):
        response = req.get(endpoint + "?page=" + str(i))
        data = response.json()
        page_items = data['payload'][target_key_name]
        page_list += page_items
    return pd.DataFrame(page_list)



def acquire_df_from_zach_api(endpoint, target_key_name):
    """
    Takes in the api endpoint and desired key (items, stores, sales) and\
    returns a dataframe with the data from the response.
    """
    response = req.get(endpoint)
    response = response.json()
    max_pages = response['payload']['max_page']
    return _create_df_from_payloads(endpoint, max_pages, target_key_name)



def merge_zach_dataframes(items_df, stores_df, sales_df):
    """
    Takes in the three data frames containing the information from the Zach API,\
    renames 'item' and 'store' columns from the sales_df to match the foreign keys\
    on the items_df and stores_df, then peforms left joins using those foreign keys.\
    Returns a single data frame containing the information from the original three.
    """
    merged_df = pd.DataFrame()
    sales_df.rename(columns={'item' : 'item_id'}, inplace=True)
    merged_df = pd.merge(items_df, sales_df, how="left", on="item_id")
    merged_df.rename(columns={'store' : 'store_id'}, inplace=True)
    merged_df = pd.merge(merged_df, stores_df, how="left", on="store_id")
    return merged_df