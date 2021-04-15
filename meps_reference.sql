--"Sex" assignments are from MEPS, source: https://meps.ahrq.gov/mepsweb/data_stats/download_data_files_codebook.jsp?PUFId=PROJYR15&varName=SEX 

SELECT DISTINCT 
    t1.dupersid,
    t2.perwtf AS person_weight,
    t1.rxndc,
    CASE WHEN t2.sex = 1 THEN 'M' 
    WHEN t2.sex = 2 THEN 'F'
    END AS gender,
    t2.agelast, --patient's last known age; advantage of using this col over other age cols is every patient has age (no NULLs)
    t2.region AS region_num
    FROM meps_prescription AS t1
    INNER JOIN meps_demographics AS t2
    ON t1.dupersid = t2.dupersid