from mdt.database import load_rxnorm, load_meps
from mdt import rxnorm
from mdt.utils import (
    rxcui_ndc_matcher,
    output_df,
    generate_module
)


def main():
    load_rxnorm()
    load_meps()

    #TODO: replace this with config settings or JSON input
    #For testing: D007037 = Hypothyroidism, D001249 = Asthma
    rxclass_id = 'D001249'
    rxclass_rela = 'may_treat'

    #Call RxClass FindClassesById API to get class info (name primarily) of the specified class
    rxclass_response = rxnorm.utils.rxapi_get_requestor(
        rxnorm.rxclass.rxclass_findclassesbyid_payload(rxclass_id)
    )
    rxclass_names =  rxnorm.utils.json_extract(rxclass_response, 'className')
    #TODO: allow for name override in input settings
    #TODO: build in better error handling if rxclass_id is garbage or returns no info
    rxclass_name = rxclass_names[0] if len(rxclass_names) > 0 else 'unspecified'

    #Call RxClass GetClassMember API to get members of the specified class with specified relationship(s)
    rxclass_response = rxnorm.utils.rxapi_get_requestor(
        rxnorm.rxclass.rxclass_getclassmember_payload(rxclass_id, rxclass_rela)
    )

    #First, get all medications that contain one of the ingredient RXCUIs
    #This will result in duplicate NDCs and potentially no MINs
    rxcui_ingredient_list = rxnorm.utils.json_extract(rxclass_response, 'rxcui')
    rxcui_ingredient_df = rxcui_ndc_matcher(rxcui_ingredient_list)

    #Second, get all of the medications that contain one of the product RXCUIs in the df above
    #This will result in potentially INs and MINs, but still duplicate NDCs
    rxcui_product_list = rxcui_ingredient_df['medication_product_rxcui'].drop_duplicates().tolist()
    rxcui_product_df = rxcui_ndc_matcher(rxcui_product_list)

    #Third, query the df above with a window function to group by NDC and prefer MIN over IN
    #This will result in only distinct NDCs that map to either an MIN (preferred) or an IN
    #https://pandas.pydata.org/pandas-docs/stable/getting_started/comparison/comparison_with_sql.html#top-n-rows-per-group
    rxcui_ndc_df = rxcui_product_df.assign(
        rn = rxcui_product_df.sort_values(['medication_ingredient_tty'], ascending=False)
        .groupby(['medication_ndc'])
        .cumcount()
        + 1
    ).query('rn < 2').drop(columns=['rn'])

    #Filter by dose form group (DFG) or dose form (DF)
    #Function expects the rxcui_ndc_df, a list of DFG or DF names, and a flag for whether to include (default) or exclude
    #If list of DFGs or DFs is empty, then nothing is filtered out
    #https://www.nlm.nih.gov/research/umls/rxnorm/docs/appendix3.html

    # Add in after adding dfg info
    # dfg_df_list = []
    # rxcui_ndc_df = filter_by_df(rxcui_ndc_df, dfg_df_list)

    #Saves df to csv
    output_df(rxcui_ndc_df)

    #Gets distributions for the rxcui_ndc_df products
    #TODO: adjust the second argument so that it'll grab the rxclass_sources (class + description, e.g., asthma_may_prevent or ATC, e.g., CCBs)
    #TODO: maybe add an input for a population_df so we can modularize MEPS in case they replace it with another population source
    generate_module(rxcui_ndc_df, rxclass_name)


if __name__ == '__main__':
    main()
