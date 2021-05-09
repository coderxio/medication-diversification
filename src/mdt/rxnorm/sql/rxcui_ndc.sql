select distinct
    sq.medication_ingredient_rxcui
    , sq.medication_ingredient_name
    , sq.medication_ingredient_tty
    , sq.medication_product_rxcui
    , sq.medication_product_name
    , sq.medication_product_tty

    , df_rxnconso.rxcui as dose_form_rxcui
    , df_rxnconso.str as dose_form_name
    , df_rxnconso.tty as dose_form_tty
    
    --, dfg_rxnconso.rxcui as dose_form_group_rxcui
    --, dfg_rxnconso.str as dose_form_group_name
    --, dfg_rxnconso.tty as dose_form_group_tty

    , ndc_rxnsat.atv as medication_ndc

from (
    
    select in_rxnconso.rxcui as medication_ingredient_rxcui
        , in_rxnconso.str as medication_ingredient_name
        , in_rxnconso.tty as medication_ingredient_tty
        , scd_rxnconso.rxcui as medication_product_rxcui
        , scd_rxnconso.str as medication_product_name
        , scd_rxnconso.tty as medication_product_tty

    -- medication ingredient (IN)
    from rxnconso in_rxnconso

    -- medication product (SCDC -> SCD)
    left join rxnrel scdc_rxnrel on scdc_rxnrel.rxcui2 = in_rxnconso.rxcui and scdc_rxnrel.rela = 'ingredient_of'
    left join rxnconso scdc_rxnconso on scdc_rxnconso.rxcui = scdc_rxnrel.rxcui1 and scdc_rxnconso.sab = 'RXNORM' and scdc_rxnconso.tty = 'SCDC'
    left join rxnrel scd_rxnrel on scd_rxnrel.rxcui2 = scdc_rxnrel.rxcui1 and scd_rxnrel.rela = 'constitutes'
    left join rxnconso scd_rxnconso on scd_rxnconso.rxcui = scd_rxnrel.rxcui1 and scd_rxnconso.sab = 'RXNORM' and scd_rxnconso.tty = 'SCD'
    
    where in_rxnconso.tty = 'IN'
        and in_rxnconso.sab = 'RXNORM'

union all 
    
    select in_rxnconso.rxcui as medication_ingredient_rxcui
        , in_rxnconso.str as medication_ingredient_name
        , in_rxnconso.tty as medication_ingredient_tty
        , sbd_rxnconso.rxcui as medication_product_rxcui
        , sbd_rxnconso.str as medication_product_name
        , sbd_rxnconso.tty as medication_product_tty

    -- medication ingredient (IN)
    from rxnconso in_rxnconso

    -- medication product (BN -> SBD)
    left join rxnrel bn_rxnrel on bn_rxnrel.rxcui2 = in_rxnconso.rxcui and bn_rxnrel.rela = 'has_tradename'
    left join rxnconso bn_rxnconso on bn_rxnconso.rxcui = bn_rxnrel.rxcui1 and bn_rxnconso.sab = 'RXNORM' and bn_rxnconso.tty = 'BN'
    left join rxnrel sbd_rxnrel on sbd_rxnrel.rxcui2 = bn_rxnrel.rxcui1 and sbd_rxnrel.rela = 'ingredient_of'
    left join rxnconso sbd_rxnconso on sbd_rxnconso.rxcui = sbd_rxnrel.rxcui1 and sbd_rxnconso.sab = 'RXNORM' and sbd_rxnconso.tty = 'SBD'
    
    where in_rxnconso.tty = 'IN'
        and in_rxnconso.sab = 'RXNORM'

union all 

    select in_rxnconso.rxcui as medication_ingredient_rxcui
        , in_rxnconso.str as medication_ingredient_name
        , in_rxnconso.tty as medication_ingredient_tty
        , gpck_rxnconso.rxcui as medication_product_rxcui
        , gpck_rxnconso.str as medication_product_name
        , gpck_rxnconso.tty as medication_product_tty

    -- medication ingredient (IN)
    from rxnconso in_rxnconso

    -- medication product (SCDC -> SCD -> GPCK)
    left join rxnrel scdc_rxnrel on scdc_rxnrel.rxcui2 = in_rxnconso.rxcui and scdc_rxnrel.rela = 'ingredient_of'
    left join rxnconso scdc_rxnconso on scdc_rxnconso.rxcui = scdc_rxnrel.rxcui1 and scdc_rxnconso.sab = 'RXNORM' and scdc_rxnconso.tty = 'SCDC'
    left join rxnrel scd_rxnrel on scd_rxnrel.rxcui2 = scdc_rxnrel.rxcui1 and scd_rxnrel.rela = 'constitutes'
    left join rxnconso scd_rxnconso on scd_rxnconso.rxcui = scd_rxnrel.rxcui1 and scd_rxnconso.sab = 'RXNORM' and scd_rxnconso.tty = 'SCD'
    left join rxnrel gpck_rxnrel on gpck_rxnrel.rxcui2 = scd_rxnrel.rxcui1 and gpck_rxnrel.rela = 'contained_in'
    left join rxnconso gpck_rxnconso on gpck_rxnconso.rxcui = gpck_rxnrel.rxcui1 and gpck_rxnconso.sab = 'RXNORM' and gpck_rxnconso.tty = 'GPCK'
    
    where in_rxnconso.tty = 'IN'
        and in_rxnconso.sab = 'RXNORM'

union all 

    select in_rxnconso.rxcui as medication_ingredient_rxcui
        , in_rxnconso.str as medication_ingredient_name
        , in_rxnconso.tty as medication_ingredient_tty
        , bpck_rxnconso.rxcui as medication_product_rxcui
        , bpck_rxnconso.str as medication_product_name
        , bpck_rxnconso.tty as medication_product_tty

    -- medication ingredient (IN)
    from rxnconso in_rxnconso

    -- medication product (SCDC -> SCD -> GPCK -> BPCK)
    left join rxnrel scdc_rxnrel on scdc_rxnrel.rxcui2 = in_rxnconso.rxcui and scdc_rxnrel.rela = 'ingredient_of'
    left join rxnconso scdc_rxnconso on scdc_rxnconso.rxcui = scdc_rxnrel.rxcui1 and scdc_rxnconso.sab = 'RXNORM' and scdc_rxnconso.tty = 'SCDC'
    left join rxnrel scd_rxnrel on scd_rxnrel.rxcui2 = scdc_rxnrel.rxcui1 and scd_rxnrel.rela = 'constitutes'
    left join rxnconso scd_rxnconso on scd_rxnconso.rxcui = scd_rxnrel.rxcui1 and scd_rxnconso.sab = 'RXNORM' and scd_rxnconso.tty = 'SCD'
    left join rxnrel gpck_rxnrel on gpck_rxnrel.rxcui2 = scd_rxnrel.rxcui1 and gpck_rxnrel.rela = 'contained_in'
    left join rxnconso gpck_rxnconso on gpck_rxnconso.rxcui = gpck_rxnrel.rxcui1 and gpck_rxnconso.sab = 'RXNORM' and gpck_rxnconso.tty = 'GPCK'
    left join rxnrel bpck_rxnrel on bpck_rxnrel.rxcui2 = gpck_rxnrel.rxcui1 and bpck_rxnrel.rela = 'has_tradename'
    left join rxnconso bpck_rxnconso on bpck_rxnconso.rxcui = bpck_rxnrel.rxcui1 and bpck_rxnconso.sab = 'RXNORM' and bpck_rxnconso.tty = 'BPCK'

    where in_rxnconso.tty = 'IN'
        and in_rxnconso.sab = 'RXNORM'

union all 

    select min_rxnconso.rxcui as medication_ingredient_rxcui
        , min_rxnconso.str as medication_ingredient_name
        , min_rxnconso.tty as medication_ingredient_tty
        , scd_rxnconso.rxcui as medication_product_rxcui
        , scd_rxnconso.str as medication_product_name
        , scd_rxnconso.tty as medication_product_tty

    -- medication ingredient (MIN)
    from rxnconso min_rxnconso

    -- medication product (SCD)
    left join rxnrel scd_rxnrel on scd_rxnrel.rxcui2 = min_rxnconso.rxcui and scd_rxnrel.rela = 'ingredients_of'
    left join rxnconso scd_rxnconso on scd_rxnconso.rxcui = scd_rxnrel.rxcui1 and scd_rxnconso.sab = 'RXNORM' and scd_rxnconso.tty = 'SCD'
    
    where min_rxnconso.tty = 'MIN'
        and min_rxnconso.sab = 'RXNORM'

union all 

    select min_rxnconso.rxcui as medication_ingredient_rxcui
        , min_rxnconso.str as medication_ingredient_name
        , min_rxnconso.tty as medication_ingredient_tty
        , sbd_rxnconso.rxcui as medication_product_rxcui
        , sbd_rxnconso.str as medication_product_name
        , sbd_rxnconso.tty as medication_product_tty

    -- medication ingredient (MIN)
    from rxnconso min_rxnconso

    -- medication product (SCD -> SBD)
    left join rxnrel scd_rxnrel on scd_rxnrel.rxcui2 = min_rxnconso.rxcui and scd_rxnrel.rela = 'ingredients_of'
    left join rxnconso scd_rxnconso on scd_rxnconso.rxcui = scd_rxnrel.rxcui1 and scd_rxnconso.sab = 'RXNORM' and scd_rxnconso.tty = 'SCD'
    left join rxnrel sbd_rxnrel on sbd_rxnrel.rxcui2 = scd_rxnrel.rxcui1 and sbd_rxnrel.rela = 'has_tradename'
    left join rxnconso sbd_rxnconso on sbd_rxnconso.rxcui = sbd_rxnrel.rxcui1 and sbd_rxnconso.sab = 'RXNORM' and sbd_rxnconso.tty = 'SBD'
    
    where min_rxnconso.tty = 'MIN'
        and min_rxnconso.sab = 'RXNORM'

union all 

    select min_rxnconso.rxcui as medication_ingredient_rxcui
        , min_rxnconso.str as medication_ingredient_name
        , min_rxnconso.tty as medication_ingredient_tty
        , gpck_rxnconso.rxcui as medication_product_rxcui
        , gpck_rxnconso.str as medication_product_name
        , gpck_rxnconso.tty as medication_product_tty

    -- medication ingredient (MIN)
    from rxnconso min_rxnconso

    -- medication product (SCD -> GPCK)
    left join rxnrel scd_rxnrel on scd_rxnrel.rxcui2 = min_rxnconso.rxcui and scd_rxnrel.rela = 'ingredients_of'
    left join rxnconso scd_rxnconso on scd_rxnconso.rxcui = scd_rxnrel.rxcui1 and scd_rxnconso.sab = 'RXNORM' and scd_rxnconso.tty = 'SCD'
    left join rxnrel gpck_rxnrel on gpck_rxnrel.rxcui2 = scd_rxnrel.rxcui1 and gpck_rxnrel.rela = 'contained_in'
    left join rxnconso gpck_rxnconso on gpck_rxnconso.rxcui = gpck_rxnrel.rxcui1 and gpck_rxnconso.sab = 'RXNORM' and gpck_rxnconso.tty = 'GPCK'
    
    where min_rxnconso.tty = 'MIN'
        and min_rxnconso.sab = 'RXNORM'

union all 

    select min_rxnconso.rxcui as medication_ingredient_rxcui
        , min_rxnconso.str as medication_ingredient_name
        , min_rxnconso.tty as medication_ingredient_tty
        , bpck_rxnconso.rxcui as medication_product_rxcui
        , bpck_rxnconso.str as medication_product_name
        , bpck_rxnconso.tty as medication_product_tty

    -- medication ingredient (MIN)
    from rxnconso min_rxnconso

    -- medication product (SCD -> SBD -> BPCK)
    left join rxnrel scd_rxnrel on scd_rxnrel.rxcui2 = min_rxnconso.rxcui and scd_rxnrel.rela = 'ingredients_of'
    left join rxnconso scd_rxnconso on scd_rxnconso.rxcui = scd_rxnrel.rxcui1 and scd_rxnconso.sab = 'RXNORM' and scd_rxnconso.tty = 'SCD'
    left join rxnrel sbd_rxnrel on sbd_rxnrel.rxcui2 = scd_rxnrel.rxcui1 and sbd_rxnrel.rela = 'has_tradename'
    left join rxnconso sbd_rxnconso on sbd_rxnconso.rxcui = sbd_rxnrel.rxcui1 and sbd_rxnconso.sab = 'RXNORM' and sbd_rxnconso.tty = 'SBD'
    left join rxnrel bpck_rxnrel on bpck_rxnrel.rxcui2 = sbd_rxnrel.rxcui1 and bpck_rxnrel.rela = 'contained_in'
    left join rxnconso bpck_rxnconso on bpck_rxnconso.rxcui = bpck_rxnrel.rxcui1 and bpck_rxnconso.sab = 'RXNORM' and bpck_rxnconso.tty = 'BPCK'
    
    where min_rxnconso.tty = 'MIN'
        and min_rxnconso.sab = 'RXNORM'
) as sq

-- dose form
left join rxnrel df_rxnrel on df_rxnrel.rxcui2 = sq.medication_product_rxcui and df_rxnrel.rela = 'has_dose_form'
left join rxnconso df_rxnconso on df_rxnconso.rxcui = df_rxnrel.rxcui1 and df_rxnconso.sab = 'RXNORM' and df_rxnconso.tty = 'DF'

-- dose form group
--left join rxnrel dfg_rxnrel on dfg_rxnrel.rxcui2 = df_rxnrel.rxcui1 and dfg_rxnrel.rela = 'isa'
--left join rxnconso dfg_rxnconso on dfg_rxnconso.rxcui = dfg_rxnrel.rxcui1 and dfg_rxnconso.sab = 'RXNORM' and dfg_rxnconso.tty = 'DFG'

-- ndc
left join rxnsat ndc_rxnsat on ndc_rxnsat.rxcui = sq.medication_product_rxcui and ndc_rxnsat.sab = 'RXNORM' and ndc_rxnsat.atn = 'NDC'

where ndc_rxnsat.atv is not null
--    and sq.medication_ingredient_rxcui in ('285155','10582','10814','10565','325521','10572')