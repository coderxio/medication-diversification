select cast(RXQUANTY as INTEGER) [RXQUANTY]
, cast(RXDAYSUP as INTEGER) [RXDAYSUP]
, rxnorm.medication_product_rxcui 
, rxnorm.medication_product_name
, count(*) [COUNT]
from meps_prescription mp 
inner join rxcui_ndc rxnorm on mp.RXNDC = rxnorm.medication_ndc 
where RXDAYSUP > 0 
group by RXQUANTY, RXDAYSUP, medication_product_name
order by count(*) desc
