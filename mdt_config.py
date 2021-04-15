import json

config = {"age": ["0-3", "4-7", "8-11", "12-18", "19-49", "50-64", "65-99"]
         }

with open('age_ranges.json', 'w') as f:
    json.dump(config, f)
    
#Age range: if user wants a drug distribution by age, Y, else N
age_range_distrib = 'Y'

#State: if user wants a distribution by state, Y, else N
state_distrib = 'Y'

#For MEPS year-specific variables (region, person_weight, age)
meps_year = '18'