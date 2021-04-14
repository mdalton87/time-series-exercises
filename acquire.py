import requests
import pandas as pd




######################################################################################################

# Functions to Acquire REST API Data

######################################################################################################



def get_items(cached=False):
    '''
    
    This function uses REST API to collect item data, then stores the data as a .csv file and returns a df
    
    '''
    if cached == False:
        items_list = []
        url = "https://python.zach.lol/api/v1/items"
        response = requests.get(url)
        data = response.json()
        n = data['payload']['max_page']
        
        for i in range(1, n+1):
            new_url = url + '?page=' + str(i)
            response = requests.get(new_url)
            data = response.json()
            page_items = data['payload']['items']
            items_list += page_items
            
        items = pd.DataFrame(items_list)
        items.to_csv('items.csv')
            
    else:
        items = pd.read_csv('items.csv', index_col=0)
    
    return items


def get_stores(cached=False):
    '''
    
    This function uses REST API to collect stores data, then stores the data as a .csv file and returns a df
    
    '''
    if cached == False:
        stores_list = []
        url = "https://python.zach.lol/api/v1/stores"
        response = requests.get(url)
        data = response.json()
        n = data['payload']['max_page']
        
        for i in range(1, n+1):
            new_url = url + '?page=' + str(i)
            response = requests.get(new_url)
            data = response.json()
            page_stores = data['payload']['stores']
            stores_list += page_stores
        
        stores = pd.DataFrame(stores_list)
        stores.to_csv('stores.csv')
        
    else:
        stores = pd.read_csv('stores.csv', index_col=0)
        
    return stores


def get_sales(cached=False):
    '''
    
    This function uses REST API to collect sales data, then stores the data as a .csv file and returns a df
    
    '''
    if cached == False:
        sales_list = []
        url = 'https://python.zach.lol/api/v1/sales'
        response = requests.get(url)
        data = response.json()
        n = data['payload']['max_page']
        
        for i in range(1, n+1):
            new_url = url + '?page=' + str(i)
            response = requests.get(new_url)
            data = response.json()
            page_sales = data['payload']['sales']
            sales_list += page_sales
            
        sales = pd.DataFrame(sales_list)
        sales.to_csv('sales.csv')
        
    else:
        sales = pd.read_csv('sales.csv', index_col=0)
    
    return sales


def complete_data(cached=False):
    '''
    
    Prior to running this function, {get_items(cached=False), get_stores(cached=False), and get_sales(cached=False)} must be ran. 
    
    This function will create a single complete dataframe from the items, sales, and stores dataframes. 
    
    '''
    if cached == False:
        items = get_items(cached=True)
        stores = get_stores(cached=True)
        sales = get_sales(cached=True)
        
        sales.columns = ['item_id', 'sale_amount', 'sale_date', 'sale_id', 'store_id']
        
        complete_data = sales.merge(stores, on='store_id')
        complete_data = complete_data.merge(items, on='item_id')
        
        complete_data.to_csv('complete_data.csv')
    
    else:
        complete_data = pd.read_csv('complete_data.csv', index_col=0)
        
    return complete_data





def get_whatyouneed(whatyouneed, cached=False):
    if cached == False:
        listy_list = []
        base_url = "https://python.zach.lol"
        api_thing = base_url + '/api/v1/'
        response = requests.get(api_thing + whatyouneed) 
        data = response.json()
        n = data['payload']['max_page']
        
        for i in range(1, n+1):
            new_url = api_thing + '?page=' + str(i)
            response = requests.get(new_url) 
            page_items = data['payload'][whatyouneed]
            listy_list += page_items
            
        whatyouneed = pd.DataFrame(listy_list)
        whatyouneed.to_csv(f'{whatyouneed}.csv')
            
    else:
        whatyouneed = pd.read_csv(f'{whatyouneed}.csv', index_col=0)
    
    return whatyouneed