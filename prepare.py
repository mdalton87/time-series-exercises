import pandas as pd
import numpy as np
import acquire as a
import datetime as dt



##########################################################################################

# Preparation of Zach's Sales Data

##########################################################################################



def prep_sales():
    '''
    
    In order to run this function: the function complete_data must have been run.
    
    This function takes output of complete_data from the acquire.py function, preps, and returns the dataframe for exploration.
    
    '''
    
    # Creates dataframe from complete_data function in acquire.py
    df = a.complete_data(cached=True)
    
    # sale_date column is converted to datetime and set as the index
    df.sale_date = pd.to_datetime(df.sale_date)
    df.set_index(df.sale_date, inplace=True)
    
    # Create the columns 'month' and 'day_of_week'
    df['month'] = df.index.month
    df['day_of_week'] = df.index.day_name()
    
    # Create 'sale_total' column
    df['sales_total'] = df.sale_amount * df.item_price
    
    return df




##########################################################################################

# Preparation of Germany Energy Consumption Data

##########################################################################################


def prep_germany(cached=False):
    '''
    
    This function pulls and preps the Germany Energy Consumption dataframe for exploration
    
    if cached == False: collects the csv from the url
    if cached == True: pulls the already saved dataframe
    
    '''
    
    if cached == False: 
        # url to opsd_germany_daily.csv
        url = 'https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv'
        # uses pull_csv function from acquire.py to collect the dataset
        df = a.pull_csv(url)
        # caches the dataset as a csv 
        df = pd.to_csv('opsd_germany_daily.csv')
        
    # cached == True
    else:
        # pulls csv as data from
        df = pd.read_csv('opsd_germany_daily.csv')
    
    # Lowercases the columns and renames 'wind+solar' columns to 'wind_and_solar'
    df.columns = df.columns.str.lower() 
    df.rename(columns={'wind+solar': 'wind_and_solar'}, inplace=True)
    
    # Conver date to datetime and set date as index
    df.date = pd.to_datetime(df.date)
    df.set_index(df.date, inplace=True)
    
    # Creates the month and year columns
    df['month'] = df.index.month
    df['year'] = df.index.year
    
    # Fills nulls with 0 
    df.fillna(0, inplace=True)
    
    return df






##########################################################################################

# Zero's and NULLs

##########################################################################################



#----------------------------------------------------------------------------------------#
###### Identifying Zeros and Nulls in columns and rows


def missing_zero_values_table(df):
    '''
    This function tales in a dataframe and counts number of Zero values and NULL values. Returns a Table with counts and percentages of each value type.
    '''
    zero_val = (df == 0.00).astype(int).sum(axis=0)
    mis_val = df.isnull().sum()
    mis_val_percent = 100 * df.isnull().sum() / len(df)
    mz_table = pd.concat([zero_val, mis_val, mis_val_percent], axis=1)
    mz_table = mz_table.rename(
    columns = {0 : 'Zero Values', 1 : 'NULL Values', 2 : '% of Total NULL Values'})
    mz_table['Total Zero\'s plus NULL Values'] = mz_table['Zero Values'] + mz_table['NULL Values']
    mz_table['% Total Zero\'s plus NULL Values'] = 100 * mz_table['Total Zero\'s plus NULL Values'] / len(df)
    mz_table['Data Type'] = df.dtypes
    mz_table = mz_table[
        mz_table.iloc[:,1] >= 0].sort_values(
    '% of Total NULL Values', ascending=False).round(1)
    print ("Your selected dataframe has " + str(df.shape[1]) + " columns and " + str(df.shape[0]) + " Rows.\n"      
        "There are " + str((mz_table['NULL Values'] != 0).sum()) +
          " columns that have NULL values.")
    #       mz_table.to_excel('D:/sampledata/missing_and_zero_values.xlsx', freeze_panes=(1,0), index = False)
    return mz_table



def missing_columns(df):
    '''
    This function takes a dataframe, counts the number of null values in each row, and converts the information into another dataframe. Adds percent of total columns.
    '''
    missing_cols_df = pd.Series(data=df.isnull().sum(axis = 1).value_counts().sort_index(ascending=False))
    missing_cols_df = pd.DataFrame(missing_cols_df)
    missing_cols_df = missing_cols_df.reset_index()
    missing_cols_df.columns = ['total_missing_cols','num_rows']
    missing_cols_df['percent_cols_missing'] = round(100 * missing_cols_df.total_missing_cols / df.shape[1], 2)
    missing_cols_df['percent_rows_affected'] = round(100 * missing_cols_df.num_rows / df.shape[0], 2)
    
    return missing_cols_df


#----------------------------------------------------------------------------------------#
###### Do things to the above zeros and nulls ^^

def handle_missing_values(df, prop_to_drop_col, prop_to_drop_row):
    '''
    This function takes in a dataframe, 
    a number between 0 and 1 that represents the proportion, for each column, of rows with non-missing values required to keep the column, 
    a another number between 0 and 1 that represents the proportion, for each row, of columns/variables with non-missing values required to keep the row, and returns the dataframe with the columns and rows dropped as indicated.
    '''
    # drop cols > thresh, axis = 1 == cols
    df = df.dropna(axis=1, thresh = prop_to_drop_col * df.shape[0])
    # drop rows > thresh, axis = 0 == rows
    df = df.dropna(axis=0, thresh = prop_to_drop_row * df.shape[1])
    return df

