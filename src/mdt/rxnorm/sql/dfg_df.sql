select distinct df_rxnconso.str as df, dfg_rxnconso.str as dfg

-- dose form
from rxnconso df_rxnconso

-- dose form group
left join rxnrel dfg_rxnrel on dfg_rxnrel.rxcui2 = df_rxnconso.rxcui and dfg_rxnrel.rela = 'isa'
left join rxnconso dfg_rxnconso on dfg_rxnconso.rxcui = dfg_rxnrel.rxcui1 and dfg_rxnconso.sab = 'RXNORM' and dfg_rxnconso.tty = 'DFG'

where df_rxnconso.sab = 'RXNORM' and df_rxnconso.tty = 'DF'
