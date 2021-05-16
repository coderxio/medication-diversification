import json
import re
import pandas as pd
from mdt.database import db_query, read_sql_string
from mdt import meps

def read_json(file_name):
    # Opening JSON file
    f = open(file_name,)

    # returns JSON object as a dictionary
    data = json.load(f)
    return data


def age_values(file_name):
    """reads age_ranges from JSON to create dataframe with age_values"""
    
    data = {}
    data['age'] = read_json('mdt_config.json')['age']
    data['age_values'] = [list(range(int(age.split('-')[0]), int(age.split('-')[1])+1)) for age in data['age']]
    df = pd.DataFrame(data)
    df = df.explode('age_values')
    return df


#TODO: Add option to string search doseage form
def rxcui_ndc_matcher(rxcui_list):
    """mashes list of RxCUIs against RxNorm combined table to get matching NDCs. 
    Select output of return, clipboard, csv....return is default"""

    df = db_query('SELECT * FROM rxcui_ndc')
    filtered_df = df[df['medication_ingredient_rxcui'].isin(rxcui_list) | df['medication_product_rxcui'].isin(rxcui_list)]
    
    print("RXCUI list matched on {0} NDCs".format(filtered_df['medication_ndc'].count()))
    
    return filtered_df


def output_df(df,output='csv', filename='df_output'):
    """Outputs a dataframe to a csv of clipboard if you use the output=clipboard arguement"""


    if output == 'clipboard':
        df.to_clipboard(index=False,excel=True)
    elif output == 'csv':
        df.to_csv('data/'+filename+'.csv',index=False)


def output_json(data, filename='json_output'):
    with open('data/'+filename+'.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def normalize_name(name):
    #Replace all non-alphanumeric characters with an underscore
    name = re.sub(r"[^a-zA-Z0-9]", "_", name)
    #Then, replace all duplicate underscores with just one underscore
    name = re.sub(r"_{2,}", "_", name)
    #If there'a an underscore at the end of the word, remove
    name = re.sub(r"_$", "", name)
    return name


def generate_module(rxcui_ndc_df, rxclass_name):
    module_dict = {}
    state_prefix = 'Prescribe_'


    rxclass_name = normalize_name(rxclass_name)
    module_dict['name'] = rxclass_name + ' Medications'
    module_dict['remarks'] = ['Remarks go here', 'and here.']
    #NOTE: not sure the difference between 1 and 2... I think 2 is the most recent version(?)
    module_dict['gmf_version'] = 2

    states_dict = {}

    #Initial state (required)
    #NOTE: if we change to conditional to check for existence of medication, channge direct_transition to transition
    states_dict['Initial'] = {
        'type': 'Initial',
        'direct_transition': state_prefix + 'Ingredient'
    }

    #Terminal state (required)
    states_dict['Terminal'] = {
        'type': 'Terminal'
    }
    
    #Get tuples of medication_product names and medication_product RXCUIs and loop through to generate MedicationOrders 

    #Read in MEPS Reference table
    meps_reference_str = read_sql_string('meps_reference.sql')
    meps_reference = db_query(meps_reference_str)

    #Join MEPS to filtered rxcui_ndc dataframe (rxcui_list)
    meps_rxcui = meps_reference.astype(str).merge(rxcui_ndc_df.astype(str)[['medication_ingredient_name', 'medication_ingredient_rxcui','medication_product_name', 'medication_product_rxcui', 'medication_ndc']], how = 'inner', left_on = 'RXNDC', right_on = 'medication_ndc')

    #Optional: Age range join - can be customized in the mdt_config.json file
    #groupby_demographic_variable: must be either an empty list [] or list of patient demographics (e.g., age, gender, state) - based on user inputs in the mdt_config.json file

    data = read_json('mdt_config.json')
    demographic_distrib_flags = data['demographic_distrib_flags']

    groupby_demographic_variables = []
    for k, v in demographic_distrib_flags.items():
        if v == 'Y':
               groupby_demographic_variables.append(k)  
        
    if demographic_distrib_flags['age'] == 'Y':
        age_ranges = age_values('mdt_config.json')
        meps_rxcui = meps_rxcui.merge(age_ranges.astype(str), how='inner', left_on='AGELAST', right_on='age_values')
    #Optional: State-region mapping from MEPS 
    if demographic_distrib_flags['state'] == 'Y':
        meps_rxcui = meps_rxcui.merge(meps.columns.meps_region_states.astype(str), how='inner', left_on='region_num', right_on='region_value')


    #Clean text to JSON/SQL-friendly format 
    for col in meps_rxcui[['medication_ingredient_name', 'medication_product_name']]:
        meps_rxcui[col] = meps_rxcui[col].apply(lambda x: normalize_name(x))

        
    dcp_dict = {}
    output = 'csv'
    medication_ingredient_list = meps_rxcui['medication_ingredient_name'].unique().tolist()
   
    #Ingredient Name Distribution (Transition 1)

    """Numerator = ingred_name
    Denominator = total population [filtered by rxclass_name upstream between rxcui_ndc & rxclass]
    1. Find distinct count of patients (DUPERSID) = patient_count
    2. Multiply count of patients * personweight = weighted_patient_count
    3. Add the weighted_patient_counts, segmented by ingredient_name + selected patient demographics = patients_by_demographics (Numerator) 
    4. Add the patients_by_demographics from Step 3 = weighted_patient_count_total (Denominator) -- Taking SUM of SUMs to make the Denominator = 100%  
    5. Calculate percentage (Output from Step 3/Output from Step 4) -- format as 0.0-1.0 per Synthea requirements. 
    6. Add the 'prescribe_' prefix to the medication_ingredient_name (e.g., 'prescribe_fluticasone') 
    7. Pivot the dataframe to transpose medication_ingredient_names from rows to columns """

    filename = rxclass_name + '_ingredient_distrib'
    #1
    dcp_dict['patient_count_ingredient'] = meps_rxcui[['medication_ingredient_name',  'medication_ingredient_rxcui', 'person_weight', 'DUPERSID']+groupby_demographic_variables].groupby(['medication_ingredient_name',  'medication_ingredient_rxcui', 'person_weight']+groupby_demographic_variables)['DUPERSID'].nunique()
    dcp_df = pd.DataFrame(dcp_dict['patient_count_ingredient']).reset_index()
    #2
    dcp_df['weighted_patient_count_ingredient'] = dcp_df['person_weight'].astype(float)*dcp_df['DUPERSID']
    #3
    dcp_dict['patients_by_demographics_ingredient'] = dcp_df.groupby(['medication_ingredient_name']+groupby_demographic_variables)['weighted_patient_count_ingredient'].sum()
    dcp_demographic_df = pd.DataFrame(dcp_dict['patients_by_demographics_ingredient']).reset_index()
    #4
    if len(groupby_demographic_variables) > 0:
        dcp_demographictotal_df = pd.merge(dcp_demographic_df,  dcp_demographic_df.groupby(groupby_demographic_variables)['weighted_patient_count_ingredient'].sum(), how = 'inner', left_on = groupby_demographic_variables, right_index=True, suffixes = ('_demographic', '_total'))
    else:
        dcp_demographictotal_df = dcp_demographic_df
        dcp_demographictotal_df['weighted_patient_count_ingredient_demographic'] = dcp_demographic_df['weighted_patient_count_ingredient']
        dcp_demographictotal_df['weighted_patient_count_ingredient_total'] = dcp_demographic_df['weighted_patient_count_ingredient'].sum()
    #5
    dcp_demographictotal_df['percent_ingredient_patients'] = round(dcp_demographictotal_df['weighted_patient_count_ingredient_demographic']/dcp_demographictotal_df['weighted_patient_count_ingredient_total'], 3)
    #6 TODO: change this column to medication_product_state_name(?)
    dcp_demographictotal_df['medication_ingredient_name'] = dcp_demographictotal_df['medication_ingredient_name'].apply(lambda x: normalize_name(state_prefix + x))
    #Generate ingredient table transition
    lookup_table_transition = []
    lookup_table_name = filename + '.' + output
    module_medication_ingredient_name_list = dcp_demographictotal_df['medication_ingredient_name'].unique().tolist()
    for idx, transition in enumerate(module_medication_ingredient_name_list):
        lookup_table_transition.append({
            'transition': transition,
            'default_probability': '1' if idx == 0 else '0',
            'lookup_table_name': lookup_table_name
        })
    state_name = state_prefix + 'Ingredient'
    states_dict[state_name] = {
        'type': 'Simple',
        'name': state_name,
        'lookup_table_transition': lookup_table_transition
    }
    #7
    dcp_dict['percent_ingredient_patients'] = dcp_demographictotal_df
    if len(groupby_demographic_variables) > 0:
        dcp_dict['percent_ingredient_patients'] = dcp_dict['percent_ingredient_patients'].reset_index().pivot(index= groupby_demographic_variables, columns = 'medication_ingredient_name', values='percent_ingredient_patients').reset_index()
    else:
        dcp_dict['percent_ingredient_patients'] = dcp_dict['percent_ingredient_patients'][['medication_ingredient_name', 'percent_ingredient_patients']].set_index('medication_ingredient_name').T
        
    #Fill NULLs and save as CSV
    dcp_dict['percent_ingredient_patients'].fillna(0, inplace=True)
    output_df(dcp_dict['percent_ingredient_patients'], output=output, filename=filename)

    #Product Name Distribution (Transition 2)
    """Numerator = product_name 
    Denominator = ingred_name
    Loop through all the ingredient_names to create product distributions by ingredient name
    Same steps as above for Ingredient Name Distribution (1-7), but first filter medication_product_names for only those that have the same medication_ingredient_name (Step 0) """

    for ingred_name in medication_ingredient_list:
        filename = rxclass_name + '_product_' + ingred_name + '_distrib'
        #0
        meps_rxcui_ingred = meps_rxcui[meps_rxcui['medication_ingredient_name']==ingred_name][['medication_product_name',  'medication_product_rxcui', 'medication_ingredient_name',  'medication_ingredient_rxcui', 'person_weight', 'DUPERSID']+groupby_demographic_variables]
        #1
        dcp_dict['patient_count_product'] = meps_rxcui_ingred.groupby(['medication_product_name',  'medication_product_rxcui',  'medication_ingredient_name',  'medication_ingredient_rxcui', 'person_weight']+groupby_demographic_variables)['DUPERSID'].nunique()
        dcp_df = pd.DataFrame(dcp_dict['patient_count_product']).reset_index()
        #2
        dcp_df['weighted_patient_count_product'] = dcp_df['person_weight'].astype(float)*dcp_df['DUPERSID']
        #3
        dcp_dict['patients_by_demographics_product'] = dcp_df.groupby(['medication_product_name', 'medication_ingredient_name']+groupby_demographic_variables)['weighted_patient_count_product'].sum()
        dcp_demographic_df = pd.DataFrame(dcp_dict['patients_by_demographics_product']).reset_index()
        #4
        dcp_demographictotal_df = pd.merge(dcp_demographic_df,  dcp_demographic_df.groupby(['medication_ingredient_name']+groupby_demographic_variables)['weighted_patient_count_product'].sum(), how = 'inner', left_on = ['medication_ingredient_name']+groupby_demographic_variables, right_index=True, suffixes = ('_demographic', '_total'))
        #5
        dcp_demographictotal_df['percent_product_patients'] = round(dcp_demographictotal_df['weighted_patient_count_product_demographic']/dcp_demographictotal_df['weighted_patient_count_product_total'], 3)
        #6 TODO: change this column to medication_product_state_name or medication_product_transition_name(?)
        dcp_demographictotal_df['medication_product_name'] = dcp_demographictotal_df['medication_product_name'].apply(lambda x: normalize_name(state_prefix + x))
        #Generate product table transition
        lookup_table_transition = []
        lookup_table_name = filename + '.' + output
        module_medication_product_name_list = dcp_demographictotal_df['medication_product_name'].unique().tolist()
        for idx, transition in enumerate(module_medication_product_name_list):
            lookup_table_transition.append({
                'transition': transition,
                'default_probability': '1' if idx == 1 else '0',
                'lookup_table_name': lookup_table_name
            })
        state_name = state_prefix + ingred_name
        states_dict[state_name] = {
            'type': 'Simple',
            'name': state_name,
            'lookup_table_transition': lookup_table_transition
        }
        #7
        dcp_dict['percent_product_patients'] = dcp_demographictotal_df
        if len(groupby_demographic_variables) > 0:
            dcp_dict['percent_product_patients'] = dcp_dict['percent_product_patients'].reset_index().pivot(index= groupby_demographic_variables, columns = 'medication_product_name', values='percent_product_patients').reset_index()
        else:
            dcp_dict['percent_product_patients'] = dcp_dict['percent_product_patients'][['medication_product_name', 'percent_product_patients']].set_index('medication_product_name').T
        
        #Fill NULLs and save as CSV 
        dcp_dict['percent_product_patients'].fillna(0, inplace=True)
        output_df(dcp_dict['percent_product_patients'], output=output, filename=filename)

    #Generate MedicationOrder states
    medication_products = list(meps_rxcui[['medication_product_name', 'medication_product_rxcui']].to_records(index=False))
    for (medication_product_name, medication_product_rxcui) in medication_products:
        state_name = normalize_name(state_prefix + medication_product_name)
        attribute = normalize_name(rxclass_name + '_prescription')
        codes = {
            'system': 'RxNorm',
            'code': medication_product_rxcui,
            'display': medication_product_name
        }
        states_dict[state_name] = {
            'type': 'MedicationOrder',
            'assign_to_attribute': attribute,
            'codes': [ codes ],
            'direct_transition': 'Terminal',
            'name': state_name
        }

    module_dict['states'] = states_dict
    
    output_json(module_dict)
