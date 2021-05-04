from .rxnorm.utils import get_dataset

from pathlib import Path
import zipfile,io, sqlite3
import pandas as pd


def to_data():
    """creates paths to data folder, making directory if not present"""
    path = Path.cwd() / 'data'
    try:
        path.mkdir(exist_ok=False)
    except:
        pass
    return path


def create_mdt_con():
    """create defualt connection to the data/MDT.db database. If database does not exist it creates it."""
    conn = sqlite3.connect(to_data() / 'MDT.db')
    return conn


def sql_create_table(table_name, df, conn=None):
    """Creates a table in the connected database when passed a pandas dataframe. 
    Note default is to delete dataframe if table name is same as global variable name that stores the df and delete_df is True"""

    if conn == None:
        conn = create_mdt_con()

    try:
        df.to_sql(table_name, conn, if_exists='replace',index=False)
        print('{} table created in DB'.format(table_name))
    except:
        print('Could not create table {0} in DB'.format(table_name))


def db_query(query_str,conn=None):
    """Sends Query to DB and returns results as a dataframe"""

    if conn == None:
       conn = create_mdt_con()

    return pd.read_sql(query_str,conn)


def read_sql_string(file_name):
    """reads the contents of a sql script into a string for python to use in a query"""

    fd = open(file_name, 'r')
    query_str  = fd.read()
    fd.close()

    print('Read {0} file as string'.format(file_name))

    return query_str


def load_rxnorm():
    """downloads and loads RxNorm dataset into database"""

    z = zipfile.ZipFile(get_dataset(handler=io.BytesIO))

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

    del z