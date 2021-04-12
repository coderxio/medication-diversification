import requests, zipfile, io
import pandas as pd
import sqlite3 as sql

#imports functions for running making database
from mdt_functions import zip_downloader, sql_create_table, db_query, read_sql_string

from meps_lists import p_col_names, p_col_spaces, d_col_names, d_col_spaces, meps_region

from config_mdt import meps_year

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
sql_create_table('meps_demographics',meps_demographics)
del meps_demographics
del z



#TODO: bring in to DB and Test
#meps_reference = db_query(f"""
#SELECT DISTINCT 
#    t1.dupersid,
#    t1.perwt{meps_year}f AS person_weight,
#    t1.rxndc,
#    CASE WHEN t2.sex = 1 THEN 'M' 
#    WHEN t2.sex = 2 THEN 'F'
#    END AS gender,
#    t2.agelast AS age, --patient's last known age; advantage of using this column is every patient is assigned an age (no NULLs)
#    t2.region{meps_year} AS region_num
#    FROM meps_prescription AS t1
#    INNER JOIN meps_demographics AS t2
#    ON t1.dupersid = t2.dupersid
#""")
#sql_create_table('meps_reference', meps_reference)
#del meps_reference

#TEST!!!!!!!!!!!!!!!! reads record count from created database
meps_prescription = db_query("Select count(*) AS records from meps_prescription")
print('DB table meps_prescription  has {0} records'.format(meps_prescription['records'].iloc[0]))

meps_demographics = db_query("Select count(*) AS records from meps_demographics")
print('DB table meps_demographics has {0} records'.format(meps_demographics['records'].iloc[0]))

#meps_reference = db_query("Select count(*) AS records meps_reference")
#print('DB table meps_reference has {0} records'.format(meps_reference['records'].iloc[0]))

meps_region = db_query("Select count(*) AS records from meps_region")
print('DB table meps_region has {0} records'.format(meps_region['records'].iloc[0]))