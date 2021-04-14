#Ingredient name Distribution (Transition 1)

import json
import pandas as pd
import numpy as np
import re
from mdt_functions import db_query, age_values,  sql_create_table, read_sql_string
from mdt_config import meps_year, age_range_distrib, state_distrib
from meps_lists import meps_region_states

#Optional: Age range join - can be customized in the age_ranges.py or age_ranges.json file
age_ranges = age_values('age_ranges.json')

#Read in MEPS Reference Table
meps_reference_str = read_sql_string('meps_reference.sql')
meps_reference = db_query(meps_reference_str)
sql_create_table('meps_reference', meps_reference)

meps_rxcui = db_query(f"""
SELECT DISTINCT mr.*
, 'prescribe_' || rn.medication_ingredient_name AS medication_ingredient_name
, rn.medication_ingredient_rxcui
, 'prescribe_' || rn.medication_product_name AS medication_product_name
, rn.medication_product_rxcui
FROM meps_reference AS mr
INNER JOIN rxcui_ndc AS rn
ON mr.rxndc = rn.medication_ndc
""")

#Optional: State-region mapping from MEPS 
if age_range_distrib == 'Y' and state_distrib == 'Y':
    meps_rxcui = meps_rxcui.astype(str).merge(age_ranges.astype(str), how='inner', left_on='age', right_on='age_values').merge(meps_region_states.astype(str), how='inner', left_on='region_num', right_on='region_value')
elif age_range_distrib == 'Y':
    meps_rxcui = meps_rxcui.astype(str).merge(age_ranges.astype(str), how='inner', left_on='age', right_on='age_values')
elif state_distrib == 'Y':
    meps_rxcui = meps_rxcui.astype(str).merge(meps_region_states.astype(str), how='inner', left_on='region_num', right_on='region_value')

#Change colnames for MDT, replace MEPS age col with age_range
meps_rxcui.drop('age', axis=1,inplace=True)
meps_rxcui.rename(columns={"age_range": "age", "states": "state"}, inplace=True)
    
#Clean text to JSON/SQL-friendly format 
for col in meps_rxcui[['medication_ingredient_name', 'medication_product_name']]:
    meps_rxcui[col] = meps_rxcui[col].str.replace(r"[^a-zA-Z0-9]", "_", regex=True).str.replace('___','_').str.replace('__','_').str.rstrip('_')




dcp_dict = {}
medication_ingredient_list = meps_rxcui['medication_ingredient_name'].unique().tolist()
disease_class = 'All' #Placeholder for Synthea module name/disease class, which should have been filtered upstream of this step

# #Using medication_ingredient, dosage form, and strength (pulled from the product_name) RxCUIs, create a distribution of medications across a therapeutic class that is segmented by age, gender, and state.
#Can adjust distribution creator parameters to segment based on product_name RxCUI instead -- to get brand vs generic. 

#Ingredient Name Distribution
#Numerator = ingred_name
#Denominator = total population [filtered by disease upstream between rxcui_ndc & rxclass]
dcp_dict['patient_count_ingredient'] = meps_rxcui[['medication_ingredient_name',  'medication_ingredient_rxcui', 'age', 'gender', 'state', 'person_weight', 'DUPERSID']].groupby(['medication_ingredient_name',  'medication_ingredient_rxcui', 'age', 'gender', 'state', 'person_weight'])['DUPERSID'].nunique()
dcp_df = pd.DataFrame(dcp_dict['patient_count_ingredient']).reset_index()
dcp_df['weighted_patient_count_ingredient'] = dcp_df['person_weight'].astype(float)*dcp_df['DUPERSID']
dcp_dict['patients_by_demographics_ingredient'] = dcp_df.groupby(['medication_ingredient_name', 'age', 'gender', 'state'])['weighted_patient_count_ingredient'].sum()
dcp_demographic_df = pd.DataFrame(dcp_dict['patients_by_demographics_ingredient']).reset_index()
 #taking SUM of SUMs to make the denominator = 100% 
dcp_demographictotal_df = pd.merge(dcp_demographic_df,  dcp_demographic_df.groupby(['age', 'gender', 'state'])['weighted_patient_count_ingredient'].sum(), how = 'inner', left_on = ['age', 'gender', 'state'], right_index=True, suffixes = ('_demographic', '_total'))
dcp_demographictotal_df['percent_ingredient_patients'] = dcp_demographictotal_df['weighted_patient_count_ingredient_demographic']/dcp_demographictotal_df['weighted_patient_count_ingredient_total']*100
dcp_dict['percent_ingredient_patients'] = dcp_demographictotal_df
dcp_dict['percent_ingredient_patients'] = dcp_dict['percent_ingredient_patients'].pivot(index= ['age', 'gender', 'state'], columns = 'medication_ingredient_name', values='percent_ingredient_patients').reset_index()

#Fill NULLs and save as CSV
#TODO: Save CSVs to a shared location or convert to alternative format that can be used by MDT module
dcp_dict['percent_ingredient_patients'].fillna(0, inplace=True)
# dcp_dict['percent_ingredient_patients'].to_csv(disease_class+'_ingredient_distrib.csv', index = False)

# # #Product Distribution
# #Numerator = product_name 
# #Denominator = ingred_name
# #Loop through all the ingredient_names to create product distributions by ingredient name 
for ingred_name in medication_ingredient_list:
    ingred_name = ingred_name.replace(r"[^a-zA-Z]", "")
    meps_rxcui_ingred = meps_rxcui.loc[meps_rxcui['medication_ingredient_name'].str.startswith(ingred_name, na=False)]
    dcp_dict['patient_count_product'] = meps_rxcui_ingred[['medication_product_name',  'medication_product_rxcui', 'medication_ingredient_name',  'medication_ingredient_rxcui', 'age', 'gender', 'state', 'person_weight', 'DUPERSID']].groupby(['medication_product_name',  'medication_product_rxcui',  'medication_ingredient_name',  'medication_ingredient_rxcui', 'age', 'gender', 'state', 'person_weight'])['DUPERSID'].nunique()
    dcp_df = pd.DataFrame(dcp_dict['patient_count_product']).reset_index()
    dcp_df['weighted_patient_count_product'] = dcp_df['person_weight'].astype(float)*dcp_df['DUPERSID']
    dcp_dict['patients_by_demographics_product'] = dcp_df.groupby(['medication_product_name', 'medication_ingredient_name', 'age', 'gender', 'state'])['weighted_patient_count_product'].sum()
    dcp_demographic_df = pd.DataFrame(dcp_dict['patients_by_demographics_product']).reset_index()
     #taking SUM of SUMs to make the denominator = 100% 
    dcp_demographictotal_df = pd.merge(dcp_demographic_df,  dcp_demographic_df.groupby(['age', 'gender', 'state', 'medication_ingredient_name'])['weighted_patient_count_product'].sum(), how = 'inner', left_on = ['age', 'gender', 'state', 'medication_ingredient_name'], right_index=True, suffixes = ('_demographic', '_total'))
    dcp_demographictotal_df['percent_product_patients'] = dcp_demographictotal_df['weighted_patient_count_product_demographic']/dcp_demographictotal_df['weighted_patient_count_product_total']*100
    dcp_dict['percent_product_patients'] = dcp_demographictotal_df
    #THIS STEP BREAKS WHEN MULTI_INGREDIENT PRODUCTS PRODUCE MULTIPLE ROWS (E.G., DROSPIRENONE, ETHINYL ESTRADIOL) --> This will be resolved upstream by the MIN/IN filters
    dcp_dict['percent_product_patients'] = dcp_dict['percent_product_patients'].reset_index().pivot(index= ['age', 'gender', 'state'], columns = 'medication_product_name', values='percent_product_patients').reset_index()

    #Fill NULLs and save as CSV 
    #CAUTION: CREATES HUNDREDS OF FILES (586 medication_ingredient_names in meps_rxcui)
    dcp_dict['percent_product_patients'].fillna(0, inplace=True)
#     dcp_dict['percent_product_patients'].to_csv(disease_class+'_product_'+ingred_name+'_distrib.csv', index = False) 
   
    