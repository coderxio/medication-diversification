import requests, zipfile, io
import pandas as pd
import sqlite3 as sql

#imports functions for downloading data making database
from mdt_functions import zip_downloader, sql_create_table, db_query, read_sql_string


#downloads urrent FDA NDCs.
z = zip_downloader('https://www.accessdata.fda.gov/cder/ndctext.zip')

#moves FDA files to sqlite database by reading as dataframes
product = pd.read_csv(z.open('product.txt'),sep='\t',dtype=object,header=0,encoding='cp1252')
package = pd.read_csv(z.open('package.txt'),sep='\t',dtype=object,header=0,encoding='cp1252')
sql_create_table('product',product)
sql_create_table('package',package)
del product
del package

#deletes FDA ZIP
del z



#NOTE: Rob's python code to join one of these tables with the rxcui_ndc table goes here
"""
rxcui_ndc_string = read_sql_string('rxcui_ndc.sql')
rxcui_ndc = db_query(rxcui_ndc_string)
sql_create_table('rxcui_ndc', rxcui_ndc)
del rxcui_ndc
"""


#TEST!!!!!!!!!!!!!!!! reads record count from created database
product = db_query("Select count(*) AS records from product limit 1")
print('DB table product has {0} records'.format(product['records'].iloc[0]))

package = db_query("Select count(*) AS records from package limit 1")
print('DB table package has {0} records'.format(package['records'].iloc[0]))
