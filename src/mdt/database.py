import importlib.resources as pkg_resources
from . import rxnorm, meps, fda
from pathlib import Path
import zipfile
import io
import sqlite3
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

    z = zipfile.ZipFile(rxnorm.utils.get_dataset(handler=io.BytesIO))

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

    rxcui_ndc = db_query(rxnorm.utils.get_sql('rxcui_ndc.sql'))
    sql_create_table('rxcui_ndc', rxcui_ndc)
    del rxcui_ndc

    dfg_df = db_query(rxnorm.utils.get_sql('dfg_df.sql'))
    sql_create_table('dfg_df', dfg_df)
    del dfg_df


def load_meps():
    '''Load Meps data into db'''
    z = zipfile.ZipFile(
        meps.utils.get_dataset('h206adat.zip', handler=io.BytesIO)
    )

    meps_prescription = pd.read_fwf(
        z.open('H206A.dat'),
        header=None,
        names=meps.columns.p_col_names,
        converters={col: str for col in meps.columns.p_col_names},
        colspecs=meps.columns.p_col_spaces,
    )

    sql_create_table('meps_prescription', meps_prescription)
    del meps_prescription
    del z

    z = zipfile.ZipFile(
        meps.utils.get_dataset('h209dat.zip', handler=io.BytesIO)
    )

    meps_demographics = pd.read_fwf(
        z.open('h209.dat'),
        header=None,
        names=meps.columns.d_col_names,
        converters={col: str for col in meps.columns.d_col_names},
        colspecs=meps.columns.d_col_spaces,
        usecols=['DUPERSID', 'PERWT18F', "REGION18", 'SEX', 'AGELAST']
    )

    # removing numbers from meps_demographic column names, since the '18' in region18 and perwt18f in MEPS are year-specific
    meps_demographics.columns = meps_demographics.columns.str.replace(r'\d+', '',regex=True)
    sql_create_table('meps_demographics', meps_demographics)
    del meps_demographics
    del z

    sql_create_table('meps_region_states', meps.columns.meps_region_states)

    meps_reference_str = meps.utils.get_sql('meps_reference.sql')
    meps_reference = db_query(meps_reference_str)
    sql_create_table('meps_reference', meps_reference)
    del meps_reference

    meps_rx_qty_ds = db_query(pkg_resources.read_text('mdt.sql', 'meps_rx_qty_ds.sql'))
    sql_create_table('meps_rx_qty_ds', meps_rx_qty_ds)
    del meps_rx_qty_ds

    # TEST!!!!!!!!!!!!!!!! reads record count from created database
    meps_prescription = db_query("Select count(*) AS records from meps_prescription")
    print('DB table meps_prescription  has {0} records'.format(meps_prescription['records'].iloc[0]))

    meps_demographics = db_query("Select count(*) AS records from meps_demographics")
    print('DB table meps_demographics has {0} records'.format(meps_demographics['records'].iloc[0]))

    meps_reference = db_query("Select count(*) AS records from meps_reference")
    print('DB table meps_reference has {0} records'.format(meps_reference['records'].iloc[0]))

    meps_region_states = db_query("Select count(*) AS records from meps_region_states")
    print('DB table meps_region_states has {0} records'.format(meps_region_states['records'].iloc[0]))

    meps_rx_qty_ds = db_query("Select count(*) AS records from meps_rx_qty_ds")
    print('DB table meps_rx_qty_ds has {0} records'.format(meps_rx_qty_ds['records'].iloc[0]))


def load_fda():
    '''Load FDA tables into db'''

    z = zipfile.ZipFile(
        fda.utils.get_dataset(handler=io.BytesIO)
    )
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
