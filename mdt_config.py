import json

config = {"age": ["0-3", "4-7", "8-11", "12-18", "19-49", "50-64", "65-99"]
         }

with open('age_ranges.json', 'w') as f:
    json.dump(config, f)
    
#If user wants a drug distribution segmented by a demographic indicator, then Y, else N
demographic_distrib_flags = {'age': 'Y', 'gender': 'Y', 'state': 'Y'}

#For MEPS year-specific variables (region, person_weight, age)
#^^resolved this upstream by stripping the year from the colnames in meps_data_reader; however, this may be handy if we want to import multiple versions of MEPS and allow user to create a year-specific distribution? 
meps_year = '18'