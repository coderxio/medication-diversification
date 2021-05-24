MEPS_CONFIG = {
    "age": ["0-3", "4-7", "8-11", "12-18", "19-49", "50-64", "65-99"],
    "demographic_distribution_flags": {"age": "Y", "gender": "Y", "state": "Y"},
    "meps_year": "18",
    "module_name": "Medication",
    "state_prefix": "Prescribe_",
    "ingredient_distribution_suffix": "_ingredient_distribution",
    "product_distribution_suffix": "_product_distribution",
    "distribution_file_type": "csv",
    "rxclass_include": [
        {
            "class_id": "R01AD",
            "relationship": "ATC"
        }
    ],
    "rxclass_exclude": [],
    "rxcui_include": [],
    "rxcui_exclude": ["25120"],
    "ingredient_tty_filter": "IN",
    "dfg_df_filter": [
        "Dry Powder Inhaler",
        "Inhalation Powder",
        "Inhalation Solution",
        "Metered Dose Inhaler"
        ]
}
