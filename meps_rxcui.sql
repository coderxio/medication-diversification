SELECT DISTINCT 
    rn.medication_ingredient_rxcui, 
    rn.medication_ingredient_name,
    rn.medication_product_name,
    REPLACE(REPLACE(
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE
    (REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
    REPLACE(rn.medication_product_name, 'a', '')
     , 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', ''), 'g', ''), 'h', ''), 'i', ''), 'j', ''), 'k', '')
     , 'l', ''), 'm', ''), 'n', ''), 'o', ''), 'p', ''), 'q', ''), 'r', ''), 's', ''), 't', ''), 'u', '')
     , 'v', ''), 'w', ''), 'x', ''), 'y', ''), 'z', ''), '[', ''), ']', '')
    AS medication_strength,
    rn.dose_form_rxcui,
    rn.dose_form_name,
    mp.dupersid,
    mp.perwt18f,
    CASE WHEN md.sex = 1 THEN 'Male' 
    	WHEN md.sex = 2 THEN 'Female'
    END AS gender,
   
   --changed at recomendation of Emily
    CASE WHEN md.age18x < 4 THEN 'Under 4'
    	WHEN md.age18x BETWEEN 4 AND 7 THEN '4 - 7'
    	WHEN md.age18x BETWEEN 8 AND 11 THEN '8 - 11'
    	WHEN md.age18x BETWEEN 12 AND 18 THEN '12 - 18'
    	WHEN md.age18x BETWEEN 19 AND 49 THEN '19 - 49'
    	WHEN md.age18x BETWEEN 50 AND 64 THEN '50 - 64'
    	WHEN md.age18x >= 65 THEN '65 and up'
    END AS age,
    mr.states AS state

    FROM rxcui_ndc AS rn --t1
    	INNER JOIN meps_prescription AS mp --t2
    								ON rn.medication_ndc = mp.rxndc
    	INNER JOIN meps_demographics AS md --t3
    								ON mp.dupersid = md.dupersid
    	INNER JOIN meps_region AS mr
    								ON md.region18 = mr.region_value

--medication was looking at the thyroid and levothryoxine by name searchs
--Original Query leverage MEPS prescription classifications of 103
;