#This scripts creates sqlite database in current script directory for all public data sources required
import requests, zipfile, io
import pandas as pd
import sqlite3 as sql
import json

def sql_create_table(table_name, df, conn=None, delete_df=True):
    """Creates a table in the connected database when passed a pandas dataframe. 
    Note default is to delete dataframe if table name is same as global variable name that stores the df and delete_df is True"""

    if conn == None:
        conn = sql.connect('MDT.db')

    try:
        df.to_sql(table_name, conn, if_exists='replace',index=False)
        print('{} table created in DB'.format(table_name))
    except:
        print('Could not create table {0} in DB'.format(table_name))

    #TODO: see about namespacing through the seperate file to fix delete issue
    #if delete_df == True:
    #    try:
    #        del globals()[table_name]
    #        print('{0} dataframe deleted from memory'.format(table_name))
    #    except:
    #        print("Could not delete dataframe from memory as {} Dataframe does not exist".format(table_name))


def zip_downloader(url):
    """Makes request to download the zip from provided URL and stores/returns content as ZipFile object"""

    r = requests.get(url)
    print('request sent to URL: {0} and returned HTTP status code of {1}'.format(url,r.status_code))

    z = zipfile.ZipFile(io.BytesIO(r.content))
    return z

def db_query(query_str,conn=None):
    """Sends Query to DB and returns results as a dataframe"""

    if conn == None:
        conn = sql.connect('MDT.db')
    
    return pd.read_sql(query_str,conn)

def read_sql_string(file_name):
    """reads the contents of a sql script into a string for python to use in a query"""

    fd = open(file_name, 'r')
    query_str  = fd.read()
    fd.close()

    print('Read {0} file as string'.format(file_name))

    return query_str

def age_values(file_name):
    """reads age_ranges JSON to create dataframe with age_values"""
    
    # Opening JSON file
    f = open(file_name,)
    
    # returns JSON object as a dictionary
    data = json.load(f)
    data['age_values'] = [list(range(int(age.split('-')[0]), int(age.split('-')[1])+1)) for age in data['age_range']]
    df = pd.DataFrame(data)
    df = df.explode('age_values')
    return df