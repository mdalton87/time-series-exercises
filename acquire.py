import requests
import pandas as pd




######################################################################################################

# Functions to Acquire REST API Data, JSON

######################################################################################################



def get_items(cached=False):
    '''
    
    This function uses REST API to collect item data, then stores the data as a .csv file and returns a df
    
    '''
    
    if cached == False:
        # create empty list
        items_list = []
        # url to items webpage
        url = "https://python.zach.lol/api/v1/items"
        # response from url
        response = requests.get(url)
        # data in json format
        data = response.json()
        # total number of pages
        n = data['payload']['max_page']
        
        # magic loop begins
        for i in range(1, n+1):
            # creates new url for the current page number
            new_url = url + '?page=' + str(i)
            # response of new url
            response = requests.get(new_url)
            # data in json from page i
            data = response.json()
            # items from page i
            page_items = data['payload']['items']
            # adds items from page i to items_list
            items_list += page_items
            
        # create items df from items_list    
        items = pd.DataFrame(items_list)
        # saves items to .csv file
        items.to_csv('items.csv')
        
    # cached == True    
    else:
        # reads items.csv
        items = pd.read_csv('items.csv', index_col=0)
    
    # returns items df
    return items


# --------------------------------------------------------------------------------------------------------------- #

def get_stores(cached=False):
    '''
    
    This function uses REST API to collect stores data, then stores the data as a .csv file and returns a df
    
    '''
    
    if cached == False:
        # create empty list
        stores_list = []
        # url to items webpage
        url = "https://python.zach.lol/api/v1/stores"
        # response from url
        response = requests.get(url)
        # data in json format
        data = response.json()
        # total number of pages
        n = data['payload']['max_page']
        
        # magic loop begins
        for i in range(1, n+1):
            # creates new url for the current page number
            new_url = url + '?page=' + str(i)
            # response of new url
            response = requests.get(new_url)
            # data in json from page i
            data = response.json()
            # stores from page i
            page_stores = data['payload']['stores']
            # adds stores from page i to stores_list
            stores_list += page_stores
        
        # create stores df from stores_list    
        stores = pd.DataFrame(stores_list)
        # saves stores to .csv file
        stores.to_csv('stores.csv')
        
        # cached == True    
    else:
        # reads stores.csv
        stores = pd.read_csv('stores.csv', index_col=0)
        
        # returns stores df
    return stores


# --------------------------------------------------------------------------------------------------------------- #


def get_sales(cached=False):
    '''
    
    This function uses REST API to collect sales data, then stores the data as a .csv file and returns a df
    
    '''
    
    if cached == False:
        # create empty list
        sales_list = []
        # url to items webpage
        url = 'https://python.zach.lol/api/v1/sales'
        # response from url
        response = requests.get(url)
        # data in json format
        data = response.json()
        # total number of pages
        n = data['payload']['max_page']
        
        # magic loop begins
        for i in range(1, n+1):
            # creates new url for the current page number
            new_url = url + '?page=' + str(i)
            # response of new url
            response = requests.get(new_url)
            # data in json from page i
            data = response.json()
            # sales from page i
            page_sales = data['payload']['sales']
            # adds sales from page i to sales_list
            sales_list += page_sales
            
        # create sales df from sales_list    
        sales = pd.DataFrame(sales_list)
        # saves sales to .csv file
        sales.to_csv('sales.csv')
        
    # cached == True    
    else:
        # reads sales.csv
        sales = pd.read_csv('sales.csv', index_col=0)
    
    # returns stores df
    return sales


# --------------------------------------------------------------------------------------------------------------- #


def complete_data(cached=False):
    '''
    
    Prior to running this function, {get_items(cached=False), get_stores(cached=False), and get_sales(cached=False)} must be ran. 
    
    This function will create a single complete dataframe from the items, sales, and stores dataframes. 
    
    '''
    if cached == False:
        # grabs the items, stores, and sales dataframes
        items = get_items(cached=True)
        stores = get_stores(cached=True)
        sales = get_sales(cached=True)
        
        # renames sales dataframe columns from item and store to item_id and store_id 
        sales.columns = ['item_id', 'sale_amount', 'sale_date', 'sale_id', 'store_id']
        
        # merges sales df and stores df on store_id col
        complete_data = sales.merge(stores, on='store_id')
        # merges complete df with items df on item_id col
        complete_data = complete_data.merge(items, on='item_id')
        
        # saves complete data to .csv file
        complete_data.to_csv('complete_data.csv')
    
    # cached == True
    else:
        # pulls already cached data
        complete_data = pd.read_csv('complete_data.csv', index_col=0)
        
    # returns complete_dataa as a dataframe
    return complete_data



######################################################################################################

# Functions to Acquire REST API Data TEXT

######################################################################################################

from io import StringIO


def pull_csv(url):
    '''
    
    This function pulls a .csv from a specified URL and returns a dataframe. 
    
    '''
    # request data from url
    req = requests.get(url)
    # converts data into string
    data = StringIO(req.text)
    # read data as csv
    df = pd.read_csv(data)
    # converts csv to dataframe
    df = pd.DataFrame(df)
    
    # returns dataframe 
    return df

