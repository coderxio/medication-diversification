import pandas as pd
from mdt_functions import db_query, sql_create_table
from mdt_variables import year

#"Sex" assignments are from MEPS, source: https://meps.ahrq.gov/mepsweb/data_stats/download_data_files_codebook.jsp?PUFId=PROJYR15&varName=SEX 
#In MEPS, perwt (pt_weight), age, and region are year-specific variables
meps_query = db_query(f"""
SELECT DISTINCT 
    t1.dupersid,
    t1.perwt{year}f AS person_weight,
    t1.rxndc,
    CASE WHEN t2.sex = 1 THEN 'M' 
    WHEN t2.sex = 2 THEN 'F'
    END AS gender,
    t2.agelast AS age, --patient's last known age; advantage of using this column is every patient is assigned an age (no NULLs)
    t2.region{year} AS region_num
    FROM meps_prescribedmeds AS t1
    INNER JOIN meps_patientdemographics AS t2
    ON t1.dupersid = t2.dupersid
""")

sql_create_table('meps_reference', meps_query)