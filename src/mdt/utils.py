import json
import re
import pandas as pd
import time
from pathlib import Path
from mdt.database import db_query, path_manager, delete_csv_files
from mdt import meps
from mdt import rxnorm


def read_json(file_name):
    # Opening JSON file
    f = open(file_name,)

    # returns JSON object as a dictionary
    data = json.load(f)
    return data


# Monkey patched this function to get run_mdt working by removing the filename arg and importing from config
def age_values(age_ranges):
    """Creates dataframe with age_values. Input is a list of age ranges (need at least 2) using the age_ranges in the settings.yaml file if populated, otherwise the default_age_ranges from mdt-settings.yaml."""

    data = {}
    data['age'] = age_ranges
    data['age_values'] = [list(range(int(age.split('-')[0]), int(age.split('-')[1])+1)) for age in data['age']]
    df = pd.DataFrame(data)
    df = df.explode('age_values')
    return df


# TODO: Add option to string search doseage form
def rxcui_ndc_matcher(rxcui_list):
    """Mashes list of RxCUIs against RxNorm combined table to get matching NDCs. 
    Select output of return, clipboard, csv....return is default"""

    df = db_query('SELECT * FROM rxcui_ndc')
    filtered_df = df[df['medication_ingredient_rxcui'].isin(rxcui_list) | df['medication_product_rxcui'].isin(rxcui_list)]

    print("RXCUI list matched on {0} NDCs".format(filtered_df['medication_ndc'].count()))

    return filtered_df


def get_prescription_details(rxcui):
    """mashes a medication product RXCUI against MEPS prescription details + RxNorm to get common prescription details.
    Either outputs False or a prescription object
    https://github.com/synthetichealth/synthea/wiki/Generic-Module-Framework%3A-States#medicationorder"""

    df = db_query('SELECT * FROM meps_rx_qty_ds')
    filtered_df = df[df['medication_product_rxcui'] == rxcui]

    # If the medication product does not have any reliable prescription details, don't generate prescription details
    # NOTE: not sure if 'return False' is the best way to do this - open to alternatives
    if len(filtered_df.index) == 0:
        return False

    # Currently, this just picks the most common prescription details at the medication product level
    # TODO: if there are more than 1 common prescription details, randomly pick one - favoring the more common ones
    selected_rx_details = filtered_df.iloc[0].to_dict()

    # NOTE: Synthea currently doesn't appear to have a field to capture quantity prescribed as part of the MedicationOrder
    rx_qty = int(selected_rx_details['RXQUANTY'])
    rx_ds = int(selected_rx_details['RXDAYSUP'])

    # TODO: maybe do this in the filtered_df step above?
    if rx_qty == 0 or rx_ds == 0:
        return False

    # See FHIR Timing reference for how these variables are calculated
    # http://hl7.org/fhir/DSTU2/datatypes.html#Timing
    frequency = int(rx_qty / rx_ds) if rx_qty >= rx_ds else 1
    period = int(rx_ds / rx_qty) if rx_ds > rx_qty else 1

    dosage = {
        'amount': 1,
        'frequency': frequency,
        'period': period,
        'unit': 'days'
    }

    duration = {
        'quantity': rx_ds,
        'unit': 'days'
    }

    prescription = {
        'dosage': dosage,
        'duration': duration
    }

    return prescription


def filter_by_dose_form(rxcui_ndc_df, settings, method='include'):
    """Gets DFs from dfg_df table that match either a DF in the list, or have a DFG that matches a DFG in the list
    If dfg_df list is empty, return the rxcui_ndc_df without filtering
    Select method option of include or exclude....include is default"""
    dose_form_filter_list = settings['dose_form_filter']
    if not isinstance(dose_form_filter_list, list):
        return rxcui_ndc_df

    dfg_df_df = db_query('SELECT * FROM dfg_df')
    filtered_dfg_df_df = dfg_df_df[dfg_df_df['dfg'].isin(dose_form_filter_list) | dfg_df_df['df'].isin(dose_form_filter_list)]
    df_list = filtered_dfg_df_df['df'].tolist()

    if method == 'include':
        filtered_rxcui_ndc_df = rxcui_ndc_df[rxcui_ndc_df['dose_form_name'].isin(df_list)]
    elif method == 'exclude':
        filtered_rxcui_ndc_df = rxcui_ndc_df[~rxcui_ndc_df['dose_form_name'].isin(df_list)]
    else:
        filtered_rxcui_ndc_df = rxcui_ndc_df

    print("RXCUI list filtered on DF matched on {0} NDCs".format(filtered_rxcui_ndc_df['medication_ndc'].count()))

    return filtered_rxcui_ndc_df

def filter_by_ingredient_tty(rxcui_ndc_df, settings):
    """Outputs a dataframe filtered by ingredient TTY"""
    ingredient_tty_filter = settings['ingredient_tty_filter']

    if ingredient_tty_filter not in ('IN', 'MIN'):
        return rxcui_ndc_df
    
    filtered_rxcui_ndc_df = rxcui_ndc_df[rxcui_ndc_df['medication_ingredient_tty'] == ingredient_tty_filter]

    return filtered_rxcui_ndc_df

def output_df(df, output='csv', path=Path.cwd(), filename='df_output'):
    """Outputs a dataframe to a csv of clipboard if you use the output=clipboard arguement"""
    filename = filename + '.' + output
    if output == 'clipboard':
        df.to_clipboard(index=False, excel=True)
    elif output == 'csv':
        df.to_csv(path / filename, index=False)


def output_json(data, path=Path.cwd(), filename='json_output'):
    filename = filename + '.json'
    with open(path / filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def output_list(data, path=Path.cwd(), filename='log'):
    timestamp = time.strftime('%Y%m%d-%H%M%S')
    filename = f'{filename} {timestamp}'
    filename = f'{filename}.txt'
    with open(path / filename, 'w', encoding = 'utf-8') as f:
        for list_item in data:
            f.write('%s\n' % list_item)


def normalize_name(name, case='camel', spaces=False):
    """ Case is optional and choices are lower, upper, and camel """

    #Replace all non-alphanumeric characters with an underscore
    name = re.sub(r"[^a-zA-Z0-9]", "_", name)
    # Then, replace all duplicate underscores with just one underscore
    name = re.sub(r"_{2,}", "_", name)
    # If there'a an underscore at the end of the word, remove
    name = re.sub(r"_$", "", name)

    if case == 'lower':
        name = name.lower()
    elif case == 'upper':
        name = name.upper()
    elif case == 'camel':
        name = name.title()

    if spaces:
        name = re.sub(r"_", " ", name)

    return name


def get_rxcui_ingredient_df(settings):
    # Call RxClass API to get all distinct members from multiple class ID / relationship pairs
    # Do this for include + add individual RXCUIs to include
    # Do this for exclude + add individual RXCUIs to exclude
    # Remove exclude RXCUIs from include RXCUI list
    rxcui_include_list = []
    rxcui_exclude_list = []

    if isinstance(settings['rxclass']['include'], list):
        rxcui_include_list = rxnorm.rxclass.rxclass_get_rxcuis(settings['rxclass']['include'])
    
    if isinstance(settings['rxcui']['include'], list):
        rxcui_include_list += settings['rxcui']['include']

    if isinstance(settings['rxclass']['exclude'], list):
        rxcui_exclude_list = rxnorm.rxclass.rxclass_get_rxcuis(settings['rxclass']['exclude'])
    
    if isinstance(settings['rxcui']['exclude'], list):
        rxcui_exclude_list += settings['rxcui']['exclude']

    rxcui_ingredient_list = [i for i in rxcui_include_list if i not in rxcui_exclude_list]
    
    rxcui_ingredient_df = rxcui_ndc_matcher(rxcui_ingredient_list)

    return rxcui_ingredient_df


def get_rxcui_product_df(rxcui_ingredient_df, settings):
    rxcui_product_list = (
        rxcui_ingredient_df["medication_product_rxcui"].drop_duplicates().tolist()
    )
    rxcui_product_df = rxcui_ndc_matcher(rxcui_product_list)

    return rxcui_product_df


def get_rxcui_ndc_df(rxcui_product_df, module_name, settings):
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
    rxcui_ndc_df = filter_by_dose_form(rxcui_ndc_df, settings)

    # Filter by ingredient term type (TTY = 'IN' or 'MIN')
    rxcui_ndc_df = filter_by_ingredient_tty(rxcui_ndc_df, settings)

    #Saves df to csv
    output_df(rxcui_ndc_df, path = path_manager(Path.cwd() / module_name / 'log'), filename='rxcui_ndc_df_output')

    return rxcui_ndc_df

def get_meps_rxcui_ndc_df(rxcui_ndc_df, module_name, settings):
    #Read in MEPS Reference table
    meps_reference = db_query(meps.utils.get_sql('meps_reference.sql'))

    #Join MEPS to filtered rxcui_ndc dataframe (rxcui_list)
    meps_rxcui_ndc_df = meps_reference.astype(str).merge(rxcui_ndc_df.astype(str)[['medication_ingredient_name', 'medication_ingredient_rxcui','medication_product_name', 'medication_product_rxcui', 'medication_ndc']], how = 'inner', left_on = 'RXNDC', right_on = 'medication_ndc')
    
    output_df(meps_rxcui_ndc_df, path = path_manager(Path.cwd() / module_name / 'log'), filename = 'meps_rxcui_ndc_df_output')

    return meps_rxcui_ndc_df

def generate_module_csv(meps_rxcui_ndc_df, module_name, settings, path=Path.cwd()):
    module = path / module_name
    lookup_tables = path_manager(module / 'lookup_tables')
    delete_csv_files(lookup_tables)

    meps_rxcui = meps_rxcui_ndc_df
    # Optional: Age range join - can be customized in the settings.yaml file
    # groupby_demographic_variable: must be either an empty list [] or list of patient demographics (e.g., age, gender, state) - based on user inputs in the settings.yaml file

    config = settings
    demographic_distribution_flags = config['meps']['demographic_distribution_flags']
    state_prefix = config['state_prefix']
    ingredient_distribution_suffix = config['ingredient_distribution_suffix']
    product_distribution_suffix = config['product_distribution_suffix']
    age_ranges = config['meps']['age_ranges']
    default_age_ranges = config['default_age_ranges']

    groupby_demographic_variables = []
    for k, v in demographic_distribution_flags.items():
        if v != False:
               groupby_demographic_variables.append(k)  
    
    # Optional: age range from MEPS 
    if demographic_distribution_flags['age'] != False:
        if not isinstance(age_ranges, list):
            age_ranges = default_age_ranges
        age_ranges_df = age_values(age_ranges)
        meps_rxcui_ndc_df = meps_rxcui_ndc_df.merge(age_ranges_df.astype(str), how='inner', left_on='AGELAST', right_on='age_values')
    
    # Optional: state-region mapping from MEPS 
    if demographic_distribution_flags['state'] != False:
        meps_rxcui_ndc_df = meps_rxcui_ndc_df.merge(meps.columns.meps_region_states.astype(str), how='inner', left_on='region_num', right_on='region_value')

    # Clean text to JSON/SQL-friendly format 
    for col in meps_rxcui_ndc_df[['medication_ingredient_name', 'medication_product_name']]:
        meps_rxcui_ndc_df[col] = meps_rxcui_ndc_df[col].apply(lambda x: normalize_name(x))

    # dcp = 'demographic count percent'    
    dcp_dict = {}
    medication_ingredient_list = meps_rxcui_ndc_df['medication_ingredient_name'].unique().tolist()
  
    # Ingredient Name Distribution (Transition 1)
    """Numerator = ingredient_name
    Denominator = total population [filtered by rxclass_name upstream between rxcui_ndc & rxclass]
    1. Find distinct count of patients (DUPERSID) = patient_count
    2. Multiply count of patients * personweight = weighted_patient_count
    3. Add the weighted_patient_counts, segmented by ingredient_name + selected patient demographics = patients_by_demographics (Numerator) 
    4. Add the patients_by_demographics from Step 3 = weighted_patient_count_total (Denominator) -- Taking SUM of SUMs to make the Denominator = 100%  
    5. Calculate percentage (Output from Step 3/Output from Step 4) -- format as 0.0-1.0 per Synthea requirements. 
    6. Add the 'prescribe_' prefix to the medication_ingredient_name (e.g., 'prescribe_fluticasone') 
    7. Pivot the dataframe to transpose medication_ingredient_names from rows to columns """

    filename = normalize_name(module_name + ingredient_distribution_suffix, 'lower')
    # 1
    dcp_dict['patient_count_ingredient'] = meps_rxcui_ndc_df[['medication_ingredient_name',  'medication_ingredient_rxcui', 'person_weight', 'DUPERSID']+groupby_demographic_variables].groupby(['medication_ingredient_name',  'medication_ingredient_rxcui', 'person_weight']+groupby_demographic_variables)['DUPERSID'].nunique()
    dcp_df = pd.DataFrame(dcp_dict['patient_count_ingredient']).reset_index()
    # 2
    dcp_df['weighted_patient_count_ingredient'] = dcp_df['person_weight'].astype(float)*dcp_df['DUPERSID']
    # 3
    dcp_dict['patients_by_demographics_ingredient'] = dcp_df.groupby(['medication_ingredient_name']+groupby_demographic_variables)['weighted_patient_count_ingredient'].sum()
    dcp_demographic_df = pd.DataFrame(dcp_dict['patients_by_demographics_ingredient']).reset_index()
    # 4
    if len(groupby_demographic_variables) > 0:
        dcp_demographictotal_ingred_df = pd.merge(dcp_demographic_df,  dcp_demographic_df.groupby(groupby_demographic_variables)['weighted_patient_count_ingredient'].sum(), how = 'inner', left_on = groupby_demographic_variables, right_index=True, suffixes = ('_demographic', '_total'))
    else:
        dcp_demographictotal_ingred_df = dcp_demographic_df
        dcp_demographictotal_ingred_df['weighted_patient_count_ingredient_demographic'] = dcp_demographic_df['weighted_patient_count_ingredient']
        dcp_demographictotal_ingred_df['weighted_patient_count_ingredient_total'] = dcp_demographic_df['weighted_patient_count_ingredient'].sum()
    # 5
    dcp_demographictotal_ingred_df['percent_ingredient_patients'] = round(dcp_demographictotal_ingred_df['weighted_patient_count_ingredient_demographic']/dcp_demographictotal_ingred_df['weighted_patient_count_ingredient_total'], 3)

    dcp_demographictotal_ingred_remarks_dict = {}
    if len(groupby_demographic_variables) > 0:
        dcp_demographictotal_ingred_remarks_df = dcp_demographictotal_ingred_df[['medication_ingredient_name', 'weighted_patient_count_ingredient_demographic']].fillna(0)
        dcp_demographictotal_ingred_remarks_df.drop_duplicates(inplace=True)
        dcp_demographictotal_ingred_remarks_dict = dcp_demographictotal_ingred_remarks_df.groupby('medication_ingredient_name')['weighted_patient_count_ingredient_demographic'].sum()
        dcp_demographictotal_ingred_remarks_df = pd.DataFrame(dcp_demographictotal_ingred_remarks_dict).reset_index().rename(columns={'weighted_patient_count_ingredient_demographic':'agg_weighted_patient_count_ingredient_demographic'})
        dcp_demographictotal_ingred_remarks_df['agg_weighted_patient_count_ingredient_total'] = dcp_demographictotal_ingred_remarks_df['agg_weighted_patient_count_ingredient_demographic'].sum()

        dcp_demographictotal_ingred_remarks_df['agg_percent_ingredient_patients'] =  round(dcp_demographictotal_ingred_remarks_df['agg_weighted_patient_count_ingredient_demographic']/dcp_demographictotal_ingred_remarks_df['agg_weighted_patient_count_ingredient_total'], 3)
    else:
        dcp_demographictotal_ingred_remarks_df = dcp_demographictotal_ingred_df[['medication_ingredient_name', 'weighted_patient_count_ingredient_demographic', 'percent_ingredient_patients']].fillna(0)
        dcp_demographictotal_ingred_remarks_df.drop_duplicates(inplace=True)
        dcp_demographictotal_ingred_remarks_df['agg_percent_ingredient_patients'] = dcp_demographictotal_ingred_remarks_df['percent_ingredient_patients']

    # 6 TODO: change this column to medication_product_state_name(?)
    dcp_dict['percent_ingredient_patients'] = dcp_demographictotal_ingred_df
    dcp_dict['percent_ingredient_patients']['medication_ingredient_transition_name'] = dcp_dict['percent_ingredient_patients']['medication_ingredient_name'].apply(lambda x: normalize_name(state_prefix + x))
    # 7
    if len(groupby_demographic_variables) > 0:
        dcp_dict['percent_ingredient_patients'] = dcp_dict['percent_ingredient_patients'].reset_index().pivot(index=groupby_demographic_variables, columns='medication_ingredient_transition_name', values='percent_ingredient_patients').reset_index()
    else:
        dcp_dict['percent_ingredient_patients'] = dcp_dict['percent_ingredient_patients'][['medication_ingredient_transition_name', 'percent_ingredient_patients']].set_index('medication_ingredient_transition_name').T
        
    # Fill NULLs and save as CSV
    dcp_dict['percent_ingredient_patients'].fillna(0, inplace=True)
    ingredient_distribution_df = dcp_dict['percent_ingredient_patients']
    output_df(ingredient_distribution_df, output = 'csv', path = path_manager(module / 'lookup_tables'), filename = filename)

    # Product Name Distribution (Transition 2)
    """Numerator = product_name 
    Denominator = ingredient_name
    Loop through all the ingredient_names to create product distributions by ingredient name
    Same steps as above for Ingredient Name Distribution (1-7), but first filter medication_product_names for only those that have the same medication_ingredient_name (Step 0) """

    # Dictionary for storing remarks %s 
    dcp_demographictotal_prod_remarks_dict = {}

    for ingredient_name in medication_ingredient_list:
        filename = normalize_name(module_name + '_' + ingredient_name + product_distribution_suffix, 'lower')
        # 0
        meps_rxcui_ingred = meps_rxcui_ndc_df[meps_rxcui_ndc_df['medication_ingredient_name']==ingredient_name][['medication_product_name',  'medication_product_rxcui', 'medication_ingredient_name', 'medication_ingredient_rxcui', 'person_weight', 'DUPERSID']+groupby_demographic_variables]
        # 1
        dcp_dict['patient_count_product'] = meps_rxcui_ingred.groupby(['medication_product_name',  'medication_product_rxcui',  'medication_ingredient_name',  'medication_ingredient_rxcui', 'person_weight']+groupby_demographic_variables)['DUPERSID'].nunique()
        dcp_df = pd.DataFrame(dcp_dict['patient_count_product']).reset_index()
        # 2
        dcp_df['weighted_patient_count_product'] = dcp_df['person_weight'].astype(float)*dcp_df['DUPERSID']
        # 3
        dcp_dict['patients_by_demographics_product'] = dcp_df.groupby(['medication_product_name', 'medication_ingredient_name']+groupby_demographic_variables)['weighted_patient_count_product'].sum()
        dcp_demographic_df = pd.DataFrame(dcp_dict['patients_by_demographics_product']).reset_index()
        # 4
        dcp_demographictotal_prod_df = pd.merge(dcp_demographic_df,  dcp_demographic_df.groupby(['medication_ingredient_name']+groupby_demographic_variables)['weighted_patient_count_product'].sum(), how = 'inner', left_on = ['medication_ingredient_name']+groupby_demographic_variables, right_index=True, suffixes = ('_demographic', '_total'))
        # 5
        dcp_demographictotal_prod_df[ingredient_name+'_percent_product_patients'] = round(dcp_demographictotal_prod_df['weighted_patient_count_product_demographic']/dcp_demographictotal_prod_df['weighted_patient_count_product_total'], 3)

        if len(groupby_demographic_variables) > 0:
            dcp_demographictotal_prod_remarks_dict[ingredient_name] = dcp_demographictotal_prod_df[['medication_product_name', 'weighted_patient_count_product_demographic']].fillna(0)
            dcp_demographictotal_prod_remarks_dict[ingredient_name].drop_duplicates(inplace=True)
            dcp_demographictotal_prod_remarks_dict[ingredient_name+'_df'] = dcp_demographictotal_prod_remarks_dict[ingredient_name].groupby('medication_product_name')['weighted_patient_count_product_demographic'].sum()
            dcp_demographictotal_prod_remarks_dict[ingredient_name+'_df_remarks'] = pd.DataFrame(dcp_demographictotal_prod_remarks_dict[ingredient_name+'_df']).reset_index().rename(columns={'weighted_patient_count_product_demographic':'agg_weighted_patient_count_product_demographic'})
            dcp_demographictotal_prod_remarks_dict[ingredient_name+'_df_remarks']['agg_weighted_patient_count_product_total'] = dcp_demographictotal_prod_remarks_dict[ingredient_name+'_df_remarks']['agg_weighted_patient_count_product_demographic'].sum()

            dcp_demographictotal_prod_remarks_dict[ingredient_name+'_df_remarks']['agg_percent_product_patients'] =  round(dcp_demographictotal_prod_remarks_dict[ingredient_name+'_df_remarks']['agg_weighted_patient_count_product_demographic']/dcp_demographictotal_prod_remarks_dict[ingredient_name+'_df_remarks']['agg_weighted_patient_count_product_total'], 3)
        else:
            dcp_demographictotal_prod_remarks_dict[ingredient_name] = dcp_demographictotal_prod_df[['medication_product_name', 'weighted_patient_count_product_demographic', ingredient_name+'_percent_product_patients']].fillna(0)
            dcp_demographictotal_prod_remarks_dict[ingredient_name].drop_duplicates(inplace=True)
            dcp_demographictotal_prod_remarks_dict[ingredient_name+'_df_remarks'] = dcp_demographictotal_prod_remarks_dict[ingredient_name]
            dcp_demographictotal_prod_remarks_dict[ingredient_name+'_df_remarks']['agg_percent_product_patients'] = dcp_demographictotal_prod_remarks_dict[ingredient_name+'_df_remarks'][ingredient_name+'_percent_product_patients']

        # 6 TODO: change this column to medication_product_state_name or medication_product_transition_name(?)
        dcp_dict['percent_product_patients'] = dcp_demographictotal_prod_df
        dcp_dict['percent_product_patients']['medication_product_transition_name'] = dcp_dict['percent_product_patients']['medication_product_name'].apply(lambda x: normalize_name(state_prefix + x))
        # 7
        if len(groupby_demographic_variables) > 0:
            dcp_dict['percent_product_patients'] = dcp_dict['percent_product_patients'].reset_index().pivot(index= groupby_demographic_variables, columns = 'medication_product_transition_name', values=ingredient_name+'_percent_product_patients').reset_index()
        else:
            dcp_dict['percent_product_patients'] = dcp_dict['percent_product_patients'][['medication_product_transition_name', ingredient_name+'_percent_product_patients']].set_index('medication_product_transition_name').T
        
        # Fill NULLs and save as CSV 
        dcp_dict['percent_product_patients'].fillna(0, inplace=True)
        product_distribution_df = dcp_dict['percent_product_patients']
        output_df(product_distribution_df, output = 'csv', path = lookup_tables, filename = filename)
    
    return dcp_demographictotal_ingred_remarks_df, dcp_demographictotal_prod_remarks_dict
    # return dcp_dict
    

def generate_module_json(meps_rxcui_ndc_df, dcp_demographictotal_ingred_remarks_df, dcp_demographictotal_prod_remarks_dict, module_name, settings, path=Path.cwd()):
    module = path / module_name

    config = settings
    demographic_distribution_flags = config['meps']['demographic_distribution_flags']
    state_prefix = config['state_prefix']
    ingredient_distribution_suffix = config['ingredient_distribution_suffix']
    product_distribution_suffix = config['product_distribution_suffix']
    as_needed = config['module']['as_needed']
    chronic = config['module']['chronic']
    refills = config['module']['refills']

    assign_to_attribute = normalize_name(module_name, case = 'lower') if config['module']['assign_to_attribute'] is None else normalize_name(config['module']['assign_to_attribute'], 'lower')
    reason = config['module']['reason']

    module_dict = {}
    all_remarks = []
    sep = '\n'
    module_display_name = config['module']['name'] if config['module']['name'] is not None else normalize_name(module_name, spaces = True)
    camelcase_module_name = normalize_name(module_display_name, spaces = True)
    uppercase_module_name = normalize_name(module_display_name, case = 'upper', spaces = True)

    module_dict['name'] = camelcase_module_name
    module_dict['remarks'] = [
        '======================================================================',
        f'  SUBMODULE {uppercase_module_name}',
        '======================================================================',
        '',
        'This submodule prescribes a medication based on population data.',
        '',
        'IT IS UP TO THE CALLING MODULE TO END THIS MEDICATION BY ATTRIBUTE.',
        'All medications prescribed in this module are assigned to the attribute',
        f'\'{assign_to_attribute}\'.',
        '',
        'Reference links:',
        '    RxClass: https://mor.nlm.nih.gov/RxClass/',
        '    RxNorm: https://www.nlm.nih.gov/research/umls/rxnorm/index.html',
        '    RxNav: https://mor.nlm.nih.gov/RxNav/',
        '    MEPS: https://meps.ahrq.gov/mepsweb/data_stats/MEPS_topics.jsp?topicid=46Z-1',
        '    FDA: https://www.fda.gov/drugs/drug-approvals-and-databases/national-drug-code-directory',
        '',
        'Made with (</>) by the CodeRx Medication Diversification Tool (MDT)'
    ]

    settings_remarks = [
        '',
        'MDT settings for this submodule:',
    ]
    settings_text = json.dumps(settings, indent = 4)
    settings_text_list = settings_text.split('\n')
    settings_remarks += settings_text_list
    module_dict['remarks'] += settings_remarks
    all_remarks += module_dict['remarks']

    # NOTE: not sure the difference between 1 and 2... I think 2 is the most recent version(?)
    module_dict['gmf_version'] = 2

    states_dict = {}

    # Initial state (required)
    # NOTE: if we change to conditional to check for existence of medication, channge direct_transition to transition
    states_dict['Initial'] = {
        'type': 'Initial',
        'conditional_transition': [
            {
                'condition': {
                    'condition_type': 'Attribute',
                    'attribute': assign_to_attribute,
                    'operator': 'is nil'
                },
                'transition': normalize_name(state_prefix + 'Ingredient')
            },
            {
                'transition': 'Terminal'
            }
        ]
    }

    # Terminal state (required)
    states_dict['Terminal'] = {
        'type': 'Terminal'
    }

    # Generate ingredient table transition
    ingredient_transition_state_remarks = [
        '======================================================================',
        ' MEDICATION INGREDIENT TABLE TRANSITION                               ',
        '======================================================================',
    ]
    ingredient_transition_state_remarks.append('Ingredients in lookup table:')
 
    for idx, row in dcp_demographictotal_ingred_remarks_df[['medication_ingredient_name', 'agg_percent_ingredient_patients']].iterrows():
        ingredient_detail = ''+str(idx+1)+' . '+str(round(row['agg_percent_ingredient_patients']*100,2))+'% '+row['medication_ingredient_name']
        ingredient_transition_state_remarks.append(ingredient_detail)

    medication_ingredient_transition_name_list = dcp_demographictotal_ingred_remarks_df['medication_ingredient_name'].apply(lambda x: normalize_name(state_prefix + x)).unique().tolist()
    filename = module_name + ingredient_distribution_suffix
    lookup_table_name = filename + '.csv'
    lookup_table_transition = []
    for idx, transition in enumerate(medication_ingredient_transition_name_list):
        lookup_table_transition.append({
            'transition': transition,
            'default_probability': '1' if idx == 0 else '0',
            'lookup_table_name': lookup_table_name
        })
    state_name = normalize_name(state_prefix + 'Ingredient')
    states_dict[state_name] = {
        'name': state_name,
        'remarks': ingredient_transition_state_remarks,
        'type': 'Simple',
        'lookup_table_transition': lookup_table_transition
    }
    all_remarks += ingredient_transition_state_remarks

    # Generate product table transition
    medication_ingredient_name_list = dcp_demographictotal_ingred_remarks_df['medication_ingredient_name'].unique().tolist()
    for ingredient_name in medication_ingredient_name_list:
        product_transition_state_remarks = [
            '======================================================================',
            ' ' + ingredient_name.upper() + ' MEDICATION PRODUCT TABLE TRANSITION  ',
            '======================================================================',
        ]
        filename = module_name + '_' + ingredient_name + product_distribution_suffix
        lookup_table_name = filename + '.csv'
        lookup_table_transition = []

        product_transition_state_remarks.append('Products in lookup table:')
        medication_product_name_list = dcp_demographictotal_prod_remarks_dict[ingredient_name+'_df_remarks']['medication_product_name'].unique().tolist()
        medication_product_name_remarks_df = dcp_demographictotal_prod_remarks_dict[ingredient_name+'_df_remarks']
        for idx, row in medication_product_name_remarks_df.iterrows():
            product_detail = ''+str(idx+1)+' . '+str(round(row['agg_percent_product_patients']*100,2))+'% '+row['medication_product_name']
            product_transition_state_remarks.append(product_detail)

        medication_product_transition_name_list = dcp_demographictotal_prod_remarks_dict[ingredient_name]['medication_product_name'].apply(lambda x: normalize_name(state_prefix + x)).unique().tolist()
        for idx, transition in enumerate(medication_product_transition_name_list):
            lookup_table_transition.append({
                'transition': transition,
                'default_probability': '1' if idx == 0 else '0',
                'lookup_table_name': lookup_table_name
            })
        state_name = normalize_name(state_prefix + ingredient_name)
        states_dict[state_name] = {
            'name': state_name,
            'remarks': product_transition_state_remarks,
            'type': 'Simple',
            'lookup_table_transition': lookup_table_transition
        }
        all_remarks += product_transition_state_remarks

    # Generate MedicationOrder states
    # medication_products = list(dcp_demographictotal_df[['medication_product_name', 'medication_product_rxcui']].to_records(index=False))
    medication_products_df = meps_rxcui_ndc_df.groupby(['medication_product_name', 'medication_product_rxcui']).size().reset_index(name='count')
    medication_products_list = medication_products_df[['medication_product_name', 'medication_product_rxcui']].values.tolist()
    #medication_products = list(medication_products_df[['medication_product_name', 'medication_product_rxcui']].to_records(index=False))

    medication_order_state_remarks = [
        '======================================================================',
        ' BEGIN MEDICATION ORDER STATES                                        ',
        '======================================================================',
    ]
    for idx, (medication_product_name, medication_product_rxcui) in enumerate(medication_products_list):
        state_name = normalize_name(state_prefix + medication_product_name)
        refills = refills if isinstance(refills, int) else 0
        codes = {
            'system': 'RxNorm',
            'code': medication_product_rxcui,
            'display': medication_product_name
        }
        prescription = {
            'refills': refills
        }
        if as_needed in (True, False):
            prescription['as_needed'] = as_needed
        states_dict[state_name] = {
            'name': state_name,
            'type': 'MedicationOrder',
            'assign_to_attribute': assign_to_attribute,
            'codes': [ codes ],
            'prescription': prescription,
            'direct_transition': 'Terminal'
        }
        if chronic in (True, False):
            states_dict[state_name]['chronic'] = chronic

        if reason is not None:
            states_dict[state_name]['reason'] = reason

        if idx == 0:
            medication_order_state_remarks_dict = {'remarks': medication_order_state_remarks}
            states_dict[state_name] = {**medication_order_state_remarks_dict, **states_dict[state_name]}

        prescription_details = get_prescription_details(medication_product_rxcui)
        if prescription_details:
            states_dict[state_name]['prescription'] = {**states_dict[state_name]['prescription'], **prescription_details}

    module_dict['states'] = states_dict
    
    filename = normalize_name(module_name, 'lower')
    output_list(all_remarks, path = path_manager(module / 'log'))
    output_json(module_dict, path = module, filename = filename)