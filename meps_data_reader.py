import requests, zipfile, io
import pandas as pd
import sqlite3 as sql

#imports functions for running making database
from mdt_functions import zip_downloader, sql_create_table, db_query, read_sql_string

from meps_lists import p_col_names, p_col_spaces, d_col_names, d_col_spaces, meps_region_states


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
    ,usecols=['DUPERSID', 'PERWT18F', "REGION18", 'SEX', 'AGELAST'])
#removing numbers from meps_demographic column names, since the '18' in region18 and perwt18f in MEPS are year-specific
meps_demographics.columns = meps_demographics.columns.str.replace(r'\d+', '',regex=True)
sql_create_table('meps_demographics',meps_demographics)
del meps_demographics
del z

sql_create_table('meps_region_states',meps_region_states)
del meps_region_states


meps_reference_str = read_sql_string('meps_reference.sql')
meps_reference = db_query(meps_reference_str)
sql_create_table('meps_reference', meps_reference)
del meps_reference


meps_rx_qty_ds_string = read_sql_string('meps_rx_qty_ds.sql')
meps_rx_qty_ds = db_query(meps_rx_qty_ds_string)
sql_create_table('meps_rx_qty_ds', meps_rx_qty_ds)
del meps_rx_qty_ds


#TEST!!!!!!!!!!!!!!!! reads record count from created database
meps_prescription = db_query("Select count(*) AS records from meps_prescription")
print('DB table meps_prescription  has {0} records'.format(meps_prescription['records'].iloc[0]))

meps_demographics = db_query("Select count(*) AS records from meps_demographics")
print('DB table meps_demographics has {0} records'.format(meps_demographics['records'].iloc[0]))

meps_region_states = db_query("Select count(*) AS records from meps_region_states")
print('DB table meps_region_states has {0} records'.format(meps_region_states['records'].iloc[0]))

meps_reference = db_query("Select count(*) AS records from meps_reference")
print('DB table meps_reference has {0} records'.format(meps_reference['records'].iloc[0]))

meps_rx_qty_ds = db_query("Select count(*) AS records from meps_rx_qty_ds")
print('DB table meps_rx_qty_ds has {0} records'.format(meps_rx_qty_ds['records'].iloc[0]))
