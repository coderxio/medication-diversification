from .utils import rxapi_get_requestor, json_extract, payload_constructor


def rxclass_getclassmember_payload(class_id, relation, ttys=['IN', 'MIN']):
    """Generates and returns URLs as strings for hitting the RxClass API function GetClassMembers."""

    relation_dict = {
        'ATC': "ATC",
        'has_EPC': "DailyMed",
        'has_Chemical_Structure': "DailyMed",
        'has_MoA': "DailyMed",
        'has_PE': "DailyMed",
        # 'has_EPC': "FDASPL", # this key is repeated
        # 'has_Chemical_Structure': "FDASPL", # this key is repeated
        # 'has_MoA':"FDASPL",
        # 'has_PE':"FDASPL",
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
        # 'has_MoA': "MEDRT",
        'has_PK': "MEDRT",
        'site_of_metabolism': "MEDRT",
        'CI_PE': "MEDRT",
        # 'has_PE': "MEDRT",
        'has_schedule': 'RXNORM',
        'MESH': "MESH",
        'isa_disposition': "SNOWMEDCT",
        'isa_structure': "SNOWMEDCT",
        'has_VAClass': "VA",
        'has_VAClass_extended': "VA",
    }

    if relation not in list(relation_dict.keys()):
        raise ValueError("results: relation must be one of %r." % list(relation_dict.keys()))

    # If relaSource is VA or RXNORM, specify ttys as one or more of: SCD, SBD, GPCK, BPCK. The default TTYs do not intersect VA or RXNORM classes.
    if relation_dict.get(relation) in ['VA', 'RXNORM']:
        ttys = ttys.extend(['SCD', 'SBD', 'GPCK', 'BPCK'])

    param_dict = {
        'classId': class_id,
        'relaSource': relation_dict.get(relation),
        'ttys': '+'.join(ttys)
    }

    # Does not send rela parameter on data sources with single rela, see RxClass API documentation
    if relation not in ['MESH', 'ATC']:
        param_dict['rela'] = relation

    payload = payload_constructor(
        'https://rxnav.nlm.nih.gov/REST/rxclass/classMembers.json?',
        param_dict
    )

    return payload


def rxclass_get_rxcuis(rxclass_query_list):
    """Returns a distinct list of rxcuis from multiple RxClass queries"""

    rxcui_list = []
    for rxclass_query in rxclass_query_list:
        class_id = rxclass_query['class_id']
        relationship = rxclass_query['relationship']

        rxclass_response = rxapi_get_requestor(
            rxclass_getclassmember_payload(class_id, relationship)
        )
        rxclass_member_list = json_extract(rxclass_response, "rxcui")
        rxcui_list += rxclass_member_list
    
    # Remove dupliate RXCUIs
    rxcui_list = list(set(rxcui_list))
    
    return rxcui_list
