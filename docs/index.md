# Documentation

This is documentation for the Medication Diversification Tool (MDT).

## Key files

### Entry point
- rx_api.py - this is the main script that runs through the MDT workflow

### Data loaders
- rxnorm_data_reader.py - gets current RxNorm data tables from NLM and stores in local database
- meps_data_reader.py - gets MEPS DAT file, converts to data frame, and stores in local database

### SQL files
- rxcui_ndc.sql - creates a view from the RxNorm tables which goes from ingredient TTY RXCUIs (IN/MIN) -> product TTY RXCUIs (SCD/GPCK/SBD/BPC) -> NDC and also includes dose form (DF) information for each product
- meps_rxcui.py - merges the MEPS prescriptions table and demographics table

### Config/utility files
- confing_mdt.py - configuration file
- mdt_functions.py - contains a lot of important functions used in other parts of MDT
- meps_lists.py - used to convert the DAT file to a dataframe
