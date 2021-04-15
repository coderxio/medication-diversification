import json
import pandas as pd
import numpy as np
import re
from mdt_functions import *
from mdt_config import meps_year, age_range_distrib, state_distrib
from meps_lists import meps_region_states

#Read in MEPS Reference table
meps_reference_str = read_sql_string('meps_reference.sql')
meps_reference = db_query(meps_reference_str)


#Join MEPS to filtered rxcui_ndc dataframe (rxcui_list)
rxcui_ndc_match = pd.read_csv('..\medication-diversification\\data\\df_output.csv')
meps_rxcui = meps_reference.astype(str).merge(rxcui_ndc_match.astype(str)[['medication_ingredient_name', 'medication_ingredient_rxcui','medication_product_name', 'medication_product_rxcui', 'medication_ndc']], how = 'inner', left_on = 'RXNDC', right_on = 'medication_ndc')


#Optional: Age range join - can be customized in the age_ranges.py or age_ranges.json file
#groupby_demographic_variable: must be either an empty list [] or list of patient demographics (e.g., age, gender, state)
groupby_demographic_variables = ['gender'] 
if age_range_distrib == 'Y':
    age_ranges = age_values('age_ranges.json')
    meps_rxcui = meps_rxcui.merge(age_ranges.astype(str), how='inner', left_on='AGELAST', right_on='age_values')
    groupby_demographic_variables.append('age')
#Optional: State-region mapping from MEPS 
if state_distrib == 'Y':
    meps_rxcui = meps_rxcui.merge(meps_region_states.astype(str), how='inner', left_on='region_num', right_on='region_value')
    groupby_demographic_variables.append('state')
groupby_demographic_variables = sorted(groupby_demographic_variables)    

#Clean text to JSON/SQL-friendly format 
for col in meps_rxcui[['medication_ingredient_name', 'medication_product_name']]:
    meps_rxcui[col] = meps_rxcui[col].str.replace(r"[^a-zA-Z0-9]", "_", regex=True).str.replace(r"_{2,}|_$",'_',regex=True)

    
dcp_dict = {}
medication_ingredient_list = meps_rxcui['medication_ingredient_name'].unique().tolist()
disease_class = 'All' #Placeholder for Synthea module name/disease class, which should have been filtered upstream of this step

#Ingredient Name Distribution (Transition 1)
"""Numerator = ingred_name
   Denominator = total population [filtered by disease upstream between rxcui_ndc & rxclass]
   1. Find distinct count of patients (DUPERSID) = patient_count
   2. Multiply count of patients * personweight = weighted_patient_count
   3. Add the weighted_patient_counts, segmented by ingredient_name + selected patient demographics = patients_by_demographics (Numerator) 
   4. Add the patients_by_demographics from Step 3 = weighted_patient_count_total (Denominator) -- Taking SUM of SUMs to make the Denominator = 100%  
   5. Calculate percentage (Output from Step 3/Output from Step 4)*100
   6. Add the 'prescribe_' prefix to the medication_ingredient_name (e.g., 'prescribe_fluticasone') 
   7. Pivot the dataframe to transpose medication_ingredient_names from rows to columns """

#1
dcp_dict['patient_count_ingredient'] = meps_rxcui[['medication_ingredient_name',  'medication_ingredient_rxcui', 'person_weight', 'DUPERSID']+groupby_demographic_variables].groupby(['medication_ingredient_name',  'medication_ingredient_rxcui', 'person_weight']+groupby_demographic_variables)['DUPERSID'].nunique()
dcp_df = pd.DataFrame(dcp_dict['patient_count_ingredient']).reset_index()
#2
dcp_df['weighted_patient_count_ingredient'] = dcp_df['person_weight'].astype(float)*dcp_df['DUPERSID']
#3
dcp_dict['patients_by_demographics_ingredient'] = dcp_df.groupby(['medication_ingredient_name']+groupby_demographic_variables)['weighted_patient_count_ingredient'].sum()
dcp_demographic_df = pd.DataFrame(dcp_dict['patients_by_demographics_ingredient']).reset_index()
#4
if groupby_demographic_variables == True:
    dcp_demographictotal_df = pd.merge(dcp_demographic_df,  dcp_demographic_df.groupby(groupby_demographic_variables)['weighted_patient_count_ingredient'].sum(), how = 'inner', left_on = groupby_demographic_variables, right_index=True, suffixes = ('_demographic', '_total'))
else:
    dcp_demographictotal_df = dcp_demographic_df
    dcp_demographictotal_df['weighted_patient_count_ingredient_demographic'] = dcp_demographic_df['weighted_patient_count_ingredient']
    dcp_demographictotal_df['weighted_patient_count_ingredient_total'] = dcp_demographic_df['weighted_patient_count_ingredient'].sum()
#5
dcp_demographictotal_df['percent_ingredient_patients'] = dcp_demographictotal_df['weighted_patient_count_ingredient_demographic']/dcp_demographictotal_df['weighted_patient_count_ingredient_total']*100
#6
dcp_demographictotal_df['medication_ingredient_name'] = 'prescribe_'+dcp_demographictotal_df['medication_ingredient_name']
#7
dcp_dict['percent_ingredient_patients'] = dcp_demographictotal_df
if groupby_demographic_variables == True:
    dcp_dict['percent_ingredient_patients'] = dcp_dict['percent_ingredient_patients'].pivot(index= groupby_demographic_variables, columns = 'medication_ingredient_name', values='percent_ingredient_patients').reset_index()
else:
    dcp_dict['percent_ingredient_patients'] = dcp_dict['percent_ingredient_patients'][['medication_ingredient_name', 'percent_ingredient_patients']].set_index('medication_ingredient_name').T
    
#Fill NULLs and save as CSV
dcp_dict['percent_ingredient_patients'].fillna(0, inplace=True)
output_df(dcp_dict['percent_ingredient_patients'], output='csv',filename=disease_class+'_ingredient_distrib')


#Product Name Distribution (Transition 2)
"""Numerator = product_name 
   Denominator = ingred_name
   Loop through all the ingredient_names to create product distributions by ingredient name
   Same steps as above for Ingredient Name Distribution (1-7), but first filter medication_product_names for only those that startwith the medication_ingredient_name (Step 0) """

for ingred_name in medication_ingredient_list:
    #0
    #TODO: Need to solve for how to handle combo drugs (e.g., fluticasone-salmeterol appears in fluticasone file, where fluticasone-only SUMs to 100% and fluticasone-salmeterol SUMs to 100%)
    ingred_name = ingred_name.replace(r"[^a-zA-Z]", "")
    meps_rxcui_ingred = meps_rxcui.loc[meps_rxcui['medication_ingredient_name'].str.startswith(ingred_name, na=False)]
    #1
    dcp_dict['patient_count_product'] = meps_rxcui_ingred[['medication_product_name',  'medication_product_rxcui', 'medication_ingredient_name',  'medication_ingredient_rxcui', 'person_weight', 'DUPERSID']+groupby_demographic_variables].groupby(['medication_product_name',  'medication_product_rxcui',  'medication_ingredient_name',  'medication_ingredient_rxcui', 'person_weight']+groupby_demographic_variables)['DUPERSID'].nunique()
    dcp_df = pd.DataFrame(dcp_dict['patient_count_product']).reset_index()
    #2
    dcp_df['weighted_patient_count_product'] = dcp_df['person_weight'].astype(float)*dcp_df['DUPERSID']
    #3
    dcp_dict['patients_by_demographics_product'] = dcp_df.groupby(['medication_product_name', 'medication_ingredient_name']+groupby_demographic_variables)['weighted_patient_count_product'].sum()
    dcp_demographic_df = pd.DataFrame(dcp_dict['patients_by_demographics_product']).reset_index()
    #4
    dcp_demographictotal_df = pd.merge(dcp_demographic_df,  dcp_demographic_df.groupby(['medication_ingredient_name']+groupby_demographic_variables)['weighted_patient_count_product'].sum(), how = 'inner', left_on = ['medication_ingredient_name']+groupby_demographic_variables, right_index=True, suffixes = ('_demographic', '_total'))
    #5
    dcp_demographictotal_df['medication_product_name'] = 'prescribe_'+dcp_demographictotal_df['medication_product_name']
    #6
    dcp_demographictotal_df['percent_product_patients'] = dcp_demographictotal_df['weighted_patient_count_product_demographic']/dcp_demographictotal_df['weighted_patient_count_product_total']*100
    #7
    dcp_dict['percent_product_patients'] = dcp_demographictotal_df
    if groupby_demographic_variables == True:
        dcp_dict['percent_product_patients'] = dcp_dict['percent_product_patients'].reset_index().pivot(index= groupby_demographic_variables, columns = 'medication_product_name', values='percent_product_patients').reset_index()
    else:
        dcp_dict['percent_product_patients'] = dcp_dict['percent_product_patients'][['medication_product_name', 'percent_product_patients']].set_index('medication_product_name').T
    
    #Fill NULLs and save as CSV 
    dcp_dict['percent_product_patients'].fillna(0, inplace=True)
    output_df(dcp_dict['percent_product_patients'],output='csv',filename=disease_class+'_product_'+ingred_name+'_distrib')
