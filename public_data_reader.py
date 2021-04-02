#This scripts creates sqlite database in current script directory for all public data sources required
import requests, zipfile, io
import pandas as pd
import sqlite3 as sql

def sql_create_table(table_name, df, conn=None, delete_df=True):
    """Creates a table in the connected database when passed a pandas dataframe. 
    Note default is to delete dataframe if table name is same as global variable name that stores the df and delete_df is True"""

    if conn == None:
        conn = sql.connect('MDT.db')

    try:
        df.to_sql(table_name, conn, if_exists='replace')
        print('{} table created in DB'.format(table_name))
    except:
        print('Could not create table {0} in DB'.format(table_name))

    if delete_df == True:
        try:
            del globals()[table_name]
            print('{0} dataframe deleted from memory'.format(table_name))
        except:
            print("Could not delete dataframe from memory as {} Dataframe does not exist".format(table_name))


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

#TODO: Figure out way to handle different years...either by reading html table or manually entery of file name.
z = zip_downloader('https://www.meps.ahrq.gov/mepsweb/data_files/pufs/h206adat.zip')


#TODO:-1 from all colspecs data since python starts a 0 and r starts at 1
col_names=['DUID', 'PID', 'DUPERSID', 'DRUGIDX', 'RXRECIDX', 'LINKIDX','PANEL', 'PURCHRD', 'RXBEGMM', 'RXBEGYRX', 'RXNAME',
    'RXDRGNAM', 'RXNDC', 'RXQUANTY', 'RXFORM', 'RXFRMUNT','RXSTRENG', 'RXSTRUNT', 'RXDAYSUP', 'PHARTP1', 'PHARTP2',
    'PHARTP3', 'PHARTP4', 'PHARTP5', 'PHARTP6', 'PHARTP7','PHARTP8', 'PHARTP9', 'RXFLG', 'IMPFLAG', 'PCIMPFLG',
    'DIABEQUIP', 'INPCFLG', 'PREGCAT', 'TC1', 'TC1S1','TC1S1_1', 'TC1S1_2', 'TC1S2', 'TC1S2_1', 'TC1S3',
    'TC1S3_1', 'TC2', 'TC2S1', 'TC2S1_1', 'TC2S1_2', 'TC2S2','TC3', 'TC3S1', 'TC3S1_1', 'RXSF18X', 'RXMR18X', 'RXMD18X',
    'RXPV18X', 'RXVA18X', 'RXTR18X', 'RXOF18X', 'RXSL18X','RXWC18X', 'RXOT18X', 'RXOR18X', 'RXOU18X', 'RXXP18X',
    'PERWT18F', 'VARSTR', 'VARPSU']
MEPS = pd.read_fwf(z.open('H206A.dat'),header=None,names=col_names,converters={col:str for col in col_names},
    colspecs=[(0,6), (7,9), (10,19), (20,32), (33,51), (52,67), (68,69), (70,70), (71,73), (74,77), (78,127), (128,187), (188,198), (199,205),
    (206,255), (256,305), (306,355), (356,405), (406,408), (409,411), (412,413), (414,415), (416,417), (418,419), (420,421), (422,423),
    (424,425), (426,427), (428,428), (429,429), (430,430), (431,431), (432,432), (433,435), (436,438), (439,441), (442,444), (445,447),
    (448,450), (451,453), (454,455), (456,457), (458,460), (461,464), (465,466), (467,469), (470,472), (473,475), (476,478), (479,481),
    (482,489), (490,497), (498,505), (506,513), (514,521), (522,528), (529,535), (536,542), (543,549), (550,557), (558,565), (566,573),
    (574,580), (581,593), (594,596), (597,None)])

sql_create_table('MEPS',MEPS)


#deletes zip for MEPS
del z


z = zip_downloader('https://download.nlm.nih.gov/rxnorm/RxNorm_full_prescribe_current.zip')

#moves RxNorm files to sqlite database by reading as dataframes
col_names = ['RXCUI','LAT','TS','LUI','STT','SUI','ISPREF','RXAUI','SAUI','SCUI','SDUI','SAB','TTY','CODE','STR','SRL','SUPPRESS','CVF','test']
RXNCONSO = pd.read_csv(z.open('rrf/RXNCONSO.RRF'),sep='|',header=None,dtype=object,names=col_names)
sql_create_table('RXNCONSO',RXNCONSO)

col_names = ['RXCUI1','RXAUI1','STYPE1','REL','RXCUI2','RXAUI2','STYPE2','RELA','RUI','SRUI','SAB','SL','DIR','RG','SUPPRESS','CVF','test']
RXNREL = pd.read_csv(z.open('rrf/RXNREL.RRF'),sep='|',dtype=object,header=None,names=col_names)
sql_create_table('RXNREL',RXNREL)

col_names = ['RXCUI','LUI','SUI','RXAUI','STYPE','CODE','ATUI','SATUI','ATN','SAB','ATV','SUPPRESS','CVF','test']
RXNSAT = pd.read_csv(z.open('rrf/RXNSAT.RRF'),sep='|',dtype=object,header=None,names=col_names)
sql_create_table('RXNSAT',RXNSAT)

#deletes RxNorm ZIP
del z

#TEST!!!!!!!!!!!!!!!! reads top line from MEPS data in database into dataframe
MEPS = db_query("Select count(*) AS records from MEPS limit 1")
print('DB table MEPS has {0} records'.format(MEPS['records'].iloc[0]))

RXNCONSO = db_query("Select count(*) AS records from RXNCONSO limit 1")
print('DB table RXNCONSO has {0} records'.format(RXNCONSO['records'].iloc[0]))

RXNREL = db_query("Select count(*) AS records from RXNREL limit 1")
print('DB table RXNREL has {0} records'.format(RXNREL['records'].iloc[0]))

RXNSAT = db_query("Select count(*) AS records from RXNSAT limit 1")
print('DB table RXNSAT has {0} records'.format(RXNSAT['records'].iloc[0]))


#TODO We could just execute a create table statement directly on the database, but this works and ported easily from joeys script
joey_query_str = read_sql_string('joey_query.sql')

joey_query = db_query(joey_query_str)
sql_create_table('joey_query', joey_query)

