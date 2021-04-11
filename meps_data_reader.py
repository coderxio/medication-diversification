import requests, zipfile, io
import pandas as pd
import sqlite3 as sql

#imports functions for running making database
from mdt_functions import zip_downloader, sql_create_table, db_query, read_sql_string

from meps_lists import p_col_names, p_col_spaces, d_col_names, d_col_spaces

#TODO: Mimic meps_r_package in python to make this cleaner and for obtaining more years worth of data.
z = zip_downloader('https://www.meps.ahrq.gov/mepsweb/data_files/pufs/h206adat.zip')


meps_prescription = pd.read_fwf(z.open('H206A.dat'),header=None,names=p_col_names,converters={col:str for col in p_col_names},
    colspecs=p_col_spaces)

sql_create_table('meps_prescription',meps_prescription)
del meps_prescription
del z


z = zip_downloader('https://www.meps.ahrq.gov/mepsweb/data_files/pufs/h209dat.zip')


meps_demographics = pd.read_fwf(z.open('h209.dat'),header=None,names=d_col_names,converters={col:str for col in d_col_names},
    colspecs=d_col_spaces
    ,usecols=['DUPERSID', 'PERWT18F', "REGION18", 'SEX', 'AGE18X'])
sql_create_table('meps_demographics',meps_demographics)
del meps_demographics
del z


#TEST!!!!!!!!!!!!!!!! reads record count from created database
meps_prescription = db_query("Select count(*) AS records from meps_prescription limit 1")
print('DB table meps_prescription  has {0} records'.format(meps_prescription['records'].iloc[0]))

meps_demographics = db_query("Select count(*) AS records from meps_demographics limit 1")
print('DB table meps_demographics has {0} records'.format(meps_demographics['records'].iloc[0]))