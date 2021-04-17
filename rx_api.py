from mdt_functions import rxclass_findclassesbyid_payload, rxclass_getclassmember_payload, rxapi_get_requestor, json_extract, rxcui_ndc_matcher, output_df, get_distributions

#TODO: replace this with config settings or JSON input
rxclass_id = 'C10AA'
rxclass_rela = 'ATC'

#Call RxClass FindClassesById API to get class info (name primarily) of the specified class
rxclass_response = rxapi_get_requestor(rxclass_findclassesbyid_payload(rxclass_id))
class_names =  json_extract(rxclass_response, 'className')
#TODO: build in better error handling if rxclass_id is garbage or returns no info
class_name = class_names[0] if len(class_names) > 0 else 'unspecified'

#Call RxClass GetClassMember API to get members of the specified class with specified relationship(s)
rxclass_response = rxapi_get_requestor(rxclass_getclassmember_payload(rxclass_id,rxclass_rela))
rxcui_list = json_extract(rxclass_response, 'rxcui')

#First match against rxcui_ndc
rxcui_ndc_match = rxcui_ndc_matcher(rxcui_list)

#Second match against to grab MINs from IN only searchs
rxcui_list = rxcui_ndc_match['medication_product_rxcui'].drop_duplicates().tolist()
rxcui_ndc_match = rxcui_ndc_matcher(rxcui_list)

#Prefer MIN over IN to get down to only distinct NDCs
#https://pandas.pydata.org/pandas-docs/stable/getting_started/comparison/comparison_with_sql.html#top-n-rows-per-group
rxcui_ndc_match = rxcui_ndc_match.assign(
    rn = rxcui_ndc_match.sort_values(['medication_ingredient_tty'], ascending=False)
    .groupby(['medication_ndc'])
    .cumcount()
    + 1
).query('rn < 2').drop(columns=['rn'])

#saves df to csv
output_df(rxcui_ndc_match)

#Gets distributions for the rxcui_ndc_match products
#TODO: adjust the second argument so that it'll grab the rxclass_sources (class + description, e.g., asthma_may_prevent or ATC, e.g., CCBs)
get_distributions(rxcui_ndc_match, class_name)

