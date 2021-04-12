import json

config = {"age_range": ["0-3", "4-7", "8-11", "12-18", "19-49", "50-64", "65-99"],  
          "age_values":  [list(range(0,4)), list(range(4,8)), list(range(8,12)), list(range(12,19)), list(range(19,50)),
          list(range(50,65)), list(range(65,100))]}

with open('age_ranges.json', 'w') as f:
    json.dump(config, f)