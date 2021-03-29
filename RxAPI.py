import requests

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


def rxclass_getclassmember_url_maker(ClassID, relation, tty = ['IN']):
    """Generates and returns URLs as strings for hitting the RxClass API function GetClassMembers."""

    base_url = 'https://rxnav.nlm.nih.gov/REST/rxclass/classMembers.json?'
    
    classid_string = 'classId=' + ClassID

#TODO: If relaSource is VA or RXNORM, specify ttys as one or more of: SCD, SBD, GPCK, BPCK. The default TTYs do not intersect VA or RXNORM classes.
    relation_source_switcher = {
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

    if relation not in list(relation_source_switcher.keys()):
        raise ValueError("results: relation must be one of %r." % list(relation_source_switcher.keys()))

    relation_source_string  = '&relaSource=' + relation_source_switcher.get(relation)
    

    #Does not send rela parameter on data sources with single rela, see RxClass API documentation
    if relation in ['MESH','ATC']:
        relation_string = ''
    else:
        relation_string = '&rela=' + relation


    #converts list of ttys into paremeter for API call, see RxClass API documentation
    for x in tty:
        tty_string = x + '+'
    
    tty_string = tty_string[0:len(tty_string)-1]
    tty_string = '&ttys=' + tty_string


    final_url = base_url + classid_string + relation_source_string + relation_string + tty_string

    return final_url


def rxapi_get_requestor(URL_string):
    """Sends a GET request to either RxNorm or RxClass"""
    response = requests.get(URL_string)

    #TODO: Add execption handling that can manage 200 responses with no JSON
    return response.json()
