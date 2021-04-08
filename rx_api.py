import requests
import urllib.parse
import pandas as pd
import sqlite3 as sql

from mdt_functions import db_query

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
    """mashes list of RxCUIs against RxNorm combined table to get matching NDCs"""

    df = db_query('SELECT * FROM rxcui_ndc')
    filtered_df = df[df['medication_ingredient_rxcui'].isin(rxcui_list) | df['medication_product_rxcui'].isin(rxcui_list)]
    
    print("RXCUI list matched on {0} NDCs and dataframe was copied to clipboard".format(filtered_df['medication_ndc'].count()))
    filtered_df.to_clipboard(index=False,excel=True)
    
    return filtered_df

#Test call below:
rxclass_response = rxapi_get_requestor(rxclass_getclassmember_payload('D001249','may_prevent'))
rxcui_list = json_extract(rxclass_response, 'rxcui')
rxcui_ndc_match = rxcui_ndc_matcher(rxcui_list)
#print('list of RXCUI returned from searcher: {0}'.format(rxcui_list))