import requests, zipfile, io
import pandas as pd
import sqlite3 as sql

#imports functions for downloading data making database
from mdt_functions import zip_downloader, sql_create_table, db_query, read_sql_string


#downloads Current RxNorm Prescribable content from nlm website.
z = zip_downloader('https://download.nlm.nih.gov/rxnorm/RxNorm_full_prescribe_current.zip')

#moves RxNorm files to sqlite database by reading as dataframes
col_names = ['RXCUI','LAT','TS','LUI','STT','SUI','ISPREF','RXAUI','SAUI','SCUI','SDUI','SAB','TTY','CODE','STR','SRL','SUPPRESS','CVF','test']
rxnconso = pd.read_csv(z.open('rrf/RXNCONSO.RRF'),sep='|',header=None,dtype=object,names=col_names)
sql_create_table('rxnconso',rxnconso)
del rxnconso

col_names = ['RXCUI1','RXAUI1','STYPE1','REL','RXCUI2','RXAUI2','STYPE2','RELA','RUI','SRUI','SAB','SL','DIR','RG','SUPPRESS','CVF','test']
rxnrel = pd.read_csv(z.open('rrf/RXNREL.RRF'),sep='|',dtype=object,header=None,names=col_names)
sql_create_table('rxnrel',rxnrel)
del rxnrel

col_names = ['RXCUI','LUI','SUI','RXAUI','STYPE','CODE','ATUI','SATUI','ATN','SAB','ATV','SUPPRESS','CVF','test']
rxnsat = pd.read_csv(z.open('rrf/RXNSAT.RRF'),sep='|',dtype=object,header=None,names=col_names)
sql_create_table('rxnsat',rxnsat)
del rxnsat 

#deletes RxNorm ZIP
del z



#Combines required rxcui elements into a single datatable for use to obtain NDCs from joeys query saved in recui_ndc.sql
rxcui_ndc_string = read_sql_string('rxcui_ndc.sql')
rxcui_ndc = db_query(rxcui_ndc_string)
sql_create_table('rxcui_ndc', rxcui_ndc)
del rxcui_ndc


#TEST!!!!!!!!!!!!!!!! reads record count from created database
RXNCONSO = db_query("Select count(*) AS records from rxnconso limit 1")
print('DB table RXNCONSO has {0} records'.format(RXNCONSO['records'].iloc[0]))

RXNREL = db_query("Select count(*) AS records from rxnrel limit 1")
print('DB table RXNREL has {0} records'.format(RXNREL['records'].iloc[0]))

RXNSAT = db_query("Select count(*) AS records from rxnsat limit 1")
print('DB table RXNSAT has {0} records'.format(RXNSAT['records'].iloc[0]))

rxcui_ndc = db_query("Select count(*) AS records from rxcui_ndc limit 1")
print('DB table rxcui_ndc has {0} records'.format(rxcui_ndc['records'].iloc[0]))


