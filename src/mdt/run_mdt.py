import sys
from pathlib import Path
from mdt.database import load_rxnorm, load_meps, load_fda, check_table
from mdt import rxnorm
from mdt.utils import (
    rxcui_ndc_matcher,
    filter_by_df,
    filter_by_ingredient_tty,
    output_df,
    get_meps_rxcui_ndc_df,
    generate_module_csv,
    generate_module_json
)
from mdt.config import MEPS_CONFIG


def main():

    if check_table('rxcui_ndc') == False:
        load_rxnorm()
    if check_table('meps_demographics') == False:
        load_meps()
    if check_table('package') == False:
        load_fda()

    # TODO: replace this with the actual user settings file
    settings = MEPS_CONFIG

    # Call RxClass API to get all distinct members from multiple class ID / relationship pairs
    # Do this for include + add individual RXCUIs to include
    # Do this for exclude + add individual RXCUIs to exclude
    # Remove exclude RXCUIs from include RXCUI list
    rxcui_include_list = rxnorm.rxclass.rxclass_get_rxcuis(settings['rxclass_include'])
    rxcui_include_list += settings['rxcui_include']

    rxcui_exclude_list = rxnorm.rxclass.rxclass_get_rxcuis(settings['rxclass_exclude'])
    rxcui_exclude_list += settings['rxcui_exclude']

    rxcui_ingredient_list = [i for i in rxcui_include_list if i not in rxcui_exclude_list]

    # First, get all medications that contain one of the ingredient RXCUIs
    # This will result in duplicate NDCs and potentially no MINs
    rxcui_ingredient_df = rxcui_ndc_matcher(rxcui_ingredient_list)

    # Second, get all of the medications that contain one of the product RXCUIs in the df above
    # This will result in potentially INs and MINs, but still duplicate NDCs
    rxcui_product_list = (
        rxcui_ingredient_df["medication_product_rxcui"].drop_duplicates().tolist()
    )
    rxcui_product_df = rxcui_ndc_matcher(rxcui_product_list)

    # Third, query the df above with a window function to group by NDC and prefer MIN over IN
    # This will result in only distinct NDCs that map to either an MIN (preferred) or an IN
    # https://pandas.pydata.org/pandas-docs/stable/getting_started/comparison/comparison_with_sql.html#top-n-rows-per-group
    rxcui_ndc_df = (
        rxcui_product_df.assign(
            rn=rxcui_product_df.sort_values(
                ["medication_ingredient_tty"], ascending=False
            )
            .groupby(["medication_ndc"])
            .cumcount()
            + 1
        )
        .query("rn < 2")
        .drop(columns=["rn"])
    )

    # Filter by dose form group (DFG) or dose form (DF)
    # Function expects the rxcui_ndc_df, a list of DFG or DF names, and a flag for whether to include (default) or exclude
    # If list of DFGs or DFs is empty, then nothing is filtered out
    # https://www.nlm.nih.gov/research/umls/rxnorm/docs/appendix3.html

    # Filter by dose form (DF) or dose form group (DFG)
    rxcui_ndc_df = filter_by_df(rxcui_ndc_df, settings['dfg_df_filter'])

    # Filter by ingredient term type (TTY = 'IN' or 'MIN')
    rxcui_ndc_df = filter_by_ingredient_tty(rxcui_ndc_df, settings['ingredient_tty_filter'])

    #Saves df to csv
    output_df(rxcui_ndc_df, filename='rxcui_ndc_df_output')

    #Join MEPS data with rxcui_ndc_df
    meps_rxcui_ndc_df = get_meps_rxcui_ndc_df(rxcui_ndc_df)

    #Generate distribution CSVs
    generate_module_csv(meps_rxcui_ndc_df)

    #Generate JSON
    generate_module_json(meps_rxcui_ndc_df)

if __name__ == "__main__":
    main()
