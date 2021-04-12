#TODO: better way to import functions?....this runs risk of override issues
from mdt_functions import *


#Test call below:
rxclass_response = rxapi_get_requestor(rxclass_getclassmember_payload('D001249','may_prevent'))
rxcui_list = json_extract(rxclass_response, 'rxcui')

#First match against rxcui_ndc
rxcui_ndc_match = rxcui_ndc_matcher(rxcui_list)

#Second match against to grab MINs from IN only searchs
rxcui_list = rxcui_ndc_match['medication_product_rxcui'].drop_duplicates().tolist()
rxcui_ndc_match = rxcui_ndc_matcher(rxcui_list)

#TODO: Pandas mugging similar to windows functions

#saves df to csv
output_df(rxcui_ndc_match)


