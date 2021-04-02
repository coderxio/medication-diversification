#This scripts creates sqlite database in current script directory for all public data sources required
import requests, zipfile, io
import pandas as pd
import sqlite3 as sql

#creates sqlite database in current directory of running script and connection path to it
conn = sql.connect('MDT.db')


#TODO: Create Function for managing the request and storing of downloaded zip
#gets MEPS data file
#TODO: Figure out way to handle different years...either by reading html table or manually entery of file name.
r = requests.get('https://www.meps.ahrq.gov/mepsweb/data_files/pufs/h206adat.zip')
z = zipfile.ZipFile(io.BytesIO(r.content))

#TODO:-1 from all colspecs data since python starts a 0 and r starts at 1
col_names=['DUID', 'PID', 'DUPERSID', 'DRUGIDX', 'RXRECIDX', 'LINKIDX','PANEL', 'PURCHRD', 'RXBEGMM', 'RXBEGYRX', 'RXNAME',
    'RXDRGNAM', 'RXNDC', 'RXQUANTY', 'RXFORM', 'RXFRMUNT','RXSTRENG', 'RXSTRUNT', 'RXDAYSUP', 'PHARTP1', 'PHARTP2',
    'PHARTP3', 'PHARTP4', 'PHARTP5', 'PHARTP6', 'PHARTP7','PHARTP8', 'PHARTP9', 'RXFLG', 'IMPFLAG', 'PCIMPFLG',
    'DIABEQUIP', 'INPCFLG', 'PREGCAT', 'TC1', 'TC1S1','TC1S1_1', 'TC1S1_2', 'TC1S2', 'TC1S2_1', 'TC1S3',
    'TC1S3_1', 'TC2', 'TC2S1', 'TC2S1_1', 'TC2S1_2', 'TC2S2','TC3', 'TC3S1', 'TC3S1_1', 'RXSF18X', 'RXMR18X', 'RXMD18X',
    'RXPV18X', 'RXVA18X', 'RXTR18X', 'RXOF18X', 'RXSL18X','RXWC18X', 'RXOT18X', 'RXOR18X', 'RXOU18X', 'RXXP18X',
    'PERWT18F', 'VARSTR', 'VARPSU']
MEPS = pd.read_fwf(z.open('H206A.dat'),header=None,names=col_names,converters={col:str for col in col_names},
    colspecs=[(1,7), (8,10), (11,20), (21,33), (34,52), (53,68), (69,70), (71,71), (72,74), (75,78), (79,128), (129,188), (189,199), (200,206),
    (207,256), (257,306), (307,356), (357,406), (407,409), (410,412), (413,414), (415,416), (417,418), (419,420), (421,422), (423,424),
    (425,426), (427,428), (429,429), (430,430), (431,431), (432,432), (433,433), (434,436), (437,439), (440,442), (443,445), (446,448),
    (449,451), (452,454), (455,456), (457,458), (459,461), (462,465), (465,467), (468,470), (471,473), (474,476), (477,479), (480,482),
    (483,490), (491,498), (499,506), (507,514), (515,522), (523,529), (530,536), (537,543), (544,550), (551,558), (559,566), (567,574),
    (574,581), (582,594), (594,597), (598,None)])

#TODO: Build function to Create Table in sqlite database and delete dataframe
#TODO: Add if exists replace table to to_sql call
MEPS.to_sql('MEPS', conn)
del MEPS

#deletes response and zipfile for MEPS
del r
del z

#downloads zip file of RxNorm Prescribe content
#TODO: figure out way to manage URLs when downloading most recent full version
r = requests.get('https://download.nlm.nih.gov/rxnorm/RxNorm_full_prescribe_03012021.zip')
z = zipfile.ZipFile(io.BytesIO(r.content))

#moves RxNorm files to sqlite database by reading as dataframes
col_names = ['RXCUI','LAT','TS','LUI','STT','SUI','ISPREF','RXAUI','SAUI','SCUI','SDUI','SAB','TTY','CODE','STR','SRL','SUPPRESS','CVF','test']
RXNCONSO = pd.read_csv(z.open('rrf/RXNCONSO.RRF'),sep='|',header=None,dtype=object,names=col_names)
RXNCONSO.to_sql('RXNCONSO', conn)
del RXNCONSO

col_names = ['RXCUI1','RXAUI1','STYPE1','REL','RXCUI2','RXAUI2','STYPE2','RELA','RUI','SRUI','SAB','SL','DIR','RG','SUPPRESS','CVF','test']
RXNREL = pd.read_csv(z.open('rrf/RXNREL.RRF'),sep='|',dtype=object,header=None,names=col_names)
RXNREL.to_sql('RXNREL', conn)
del RXNREL

col_names = ['RXCUI','LUI','SUI','RXAUI','STYPE','CODE','ATUI','SATUI','ATN','SAB','ATV','SUPPRESS','CVF','test']
RXNSAT = pd.read_csv(z.open('rrf/RXNSAT.RRF'),sep='|',dtype=object,header=None,names=col_names)
RXNSAT.to_sql('RXNSAT', conn)
del RXNSAT

#deletes RxNorm repsonse and ZIP
del r
del z

#TEST!!!!!!!!!!!!!!!! reads top line from MEPS data in database into dataframe
MEPS = pd.read_sql("Select count(*) AS records from MEPS limit 1",conn)
print('MEPS has {0} records'.format(MEPS['records'].iloc[0]))
RXNCONSO = pd.read_sql("Select count(*) AS records from RXNCONSO limit 1",conn)
print('RXNCONSO has {0} records'.format(RXNCONSO['records'].iloc[0]))
RXNREL = pd.read_sql("Select count(*) AS records from RXNREL limit 1",conn)
print('RXNREL has {0} records'.format(RXNREL['records'].iloc[0]))
RXNSAT = pd.read_sql("Select count(*) AS records from RXNSAT limit 1",conn)
print('RXNSAT has {0} records'.format(RXNSAT['records'].iloc[0]))

#TODO execute sql to build Joeys RxNorm lookup table