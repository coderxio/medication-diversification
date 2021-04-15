#This scripts creates sqlite database in current script directory for all public data sources required
import requests, zipfile, io
import pandas as pd
import sqlite3 as sql

import json

import urllib.parse


#data gathering and database functions
def create_mdt_con():
    """create defualt connection to the data/MDT.db database. If database does not exist it creates it."""
    conn = sql.connect('data/MDT.db')
    return conn

def sql_create_table(table_name, df, conn=None, delete_df=True):
    """Creates a table in the connected database when passed a pandas dataframe. 
    Note default is to delete dataframe if table name is same as global variable name that stores the df and delete_df is True"""

    if conn == None:
        conn = create_mdt_con()

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
       conn = create_mdt_con()
    
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
    data['age_values'] = [list(range(int(age.split('-')[0]), int(age.split('-')[1])+1)) for age in data['age']]
    df = pd.DataFrame(data)
    df = df.explode('age_values')
    return df



#api functions
def json_extract(obj, key):
    """Recursively fetch values from nested JSON."""
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    values = extract(obj, arr, key)
    print(values)
    return values


def payload_constructor(base_url,params):
    #TODO: exception handling for params as dict

    params_str = urllib.parse.urlencode(params, safe=':+')
    payload = {'base_url':base_url,
                'params':params_str}

    #debug print out
    print("""payload built with base URL: {0} and parameters: {1}""".format(base_url,params_str))

    return payload


def rxclass_getclassmember_payload(class_id, relation, ttys = ['IN','MIN']):
    """Generates and returns URLs as strings for hitting the RxClass API function GetClassMembers."""

    relation_dict = {
        'ATC':"ATC",
        'has_EPC':"DailyMed",
        'has_Chemical_Structure':"DailyMed",
        'has_MoA':"DailyMed",
        'has_PE':"DailyMed",
        'has_EPC':"FDASPL",
        'has_Chemical_Structure':"FDASPL",
        #'has_MoA':"FDASPL",
        #'has_PE':"FDASPL",
        'has_TC': "FMTSME",
        'CI_with': "MEDRT",
        'induces': "MEDRT",
        'may_diagnose': "MEDRT",
        'may_prevent': "MEDRT",
        'may_treat': "MEDRT",
        'CI_ChemClass': "MEDRT",
        'has_active_metabolites': "MEDRT",
        'has_Ingredient': "MEDRT",
        'CI_MoA': "MEDRT",
        #'has_MoA': "MEDRT",
        'has_PK': "MEDRT",
        'site_of_metabolism': "MEDRT",
        'CI_PE': "MEDRT",
        #'has_PE': "MEDRT",
        'has_schedule':'RXNORM',
        'MESH': "MESH",
        'isa_disposition': "SNOWMEDCT",
        'isa_structure': "SNOWMEDCT",
        'has_VAClass': "VA",
        'has_VAClass_extended': "VA",
    }

    if relation not in list(relation_dict.keys()):
        raise ValueError("results: relation must be one of %r." % list(relation_dict.keys()))

    #If relaSource is VA or RXNORM, specify ttys as one or more of: SCD, SBD, GPCK, BPCK. The default TTYs do not intersect VA or RXNORM classes.
    if relation_dict.get(relation) in ['VA','RXNORM']:
        ttys = ttys.extend(['SCD','SBD','GPCK','BPCK'])


    param_dict = {'classId':class_id,
                  'relaSource':relation_dict.get(relation),
                  'ttys':'+'.join(ttys)}

    #Does not send rela parameter on data sources with single rela, see RxClass API documentation
    if relation not in ['MESH','ATC']:
        param_dict['rela'] = relation 
    
    payload = payload_constructor('https://rxnav.nlm.nih.gov/REST/rxclass/classMembers.json?', param_dict)

    return payload


def rxapi_get_requestor(request_dict):
    """Sends a GET request to either RxNorm or RxClass"""
    response = requests.get(request_dict['base_url'],params=request_dict['params'])

    #debug print out
    print("GET Request sent to URL: {0}".format(response.url))
    print("Response HTTP Code: {0}".format(response.status_code))
    if response.status_code == 200:
    #TODO: Add execption handling that can manage 200 responses with no JSON
        return response.json()


#TODO: add loop to flip through search lists and combine them into final list
def searcher(search_list):
    """loops through a list of tuples and join resulting RXCUIs lists in a list"""
    rxcui_list = []
    for x in search_list:
       api_response = rxapi_get_requestor(rxclass_getclassmember_payload(x[0],x[1]))
       return_rxcui = json_extract(api_response,'rxcui')
       rxcui_list.append(return_rxcui)
  
    return rxcui_list


#TODO: Add option to string search doseage form
def rxcui_ndc_matcher(rxcui_list):
    """mashes list of RxCUIs against RxNorm combined table to get matching NDCs. 
    Select output of return, clipboard, csv....return is default"""

    df = db_query('SELECT * FROM rxcui_ndc')
    filtered_df = df[df['medication_ingredient_rxcui'].isin(rxcui_list) | df['medication_product_rxcui'].isin(rxcui_list)]
    
    print("RXCUI list matched on {0} NDCs".format(filtered_df['medication_ndc'].count()))
    
    return filtered_df

def output_df(df,output='csv',filename='df_output'):
    """Outputs a dataframe to a csv of clipboard if you use the output=clipboard arguement"""


    if output == 'clipboard':
        df.to_clipboard(index=False,excel=True)
    elif output == 'csv':
        df.to_csv('data/'+filename+'.csv',index=False)
