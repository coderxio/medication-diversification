import sys
import argparse
from mdt.database import (
    load_rxnorm,
    load_meps,
    load_fda,
    check_table,
)
from mdt.yamlmanager import (
    create_mdt_settings,
    create_module_settings,
    get_settings,
)
from mdt.utils import (
    get_rxcui_ingredient_df,
    get_rxcui_product_df,
    get_rxcui_ndc_df,
    get_meps_rxcui_ndc_df,
    generate_module_csv,
    generate_module_json,
)


def init_db(args):
    if check_table('rxcui_ndc') is False:
        load_rxnorm()
    
    if check_table('meps_demographics') is False:
        load_meps()
    
    if check_table('package') is False:
        load_fda()

    print('All Tables are loaded')

    create_mdt_settings()


def module_create(args):
    arguments = vars(args)
    create_module_settings(arguments['module_name'])


def module_build(args):
    arguments = vars(args)
    module_name = arguments['module_name']
    settings = get_settings(module_name)

    # First, get all medications that contain one of the ingredient RXCUIs
    # This will result in duplicate NDCs and potentially no MINs
    rxcui_ingredient_df = get_rxcui_ingredient_df(settings)

    # Second, get all of the medications that contain one of the product RXCUIs in the df above
    # This will result in potentially INs and MINs, but still duplicate NDCs
    rxcui_product_df = get_rxcui_product_df(rxcui_ingredient_df, settings)

    # Third, query the rxcui_product_df with a window function to group by NDC and prefer MIN over IN
    # This will result in only distinct NDCs that map to either an MIN (preferred) or an IN
    # https://pandas.pydata.org/pandas-docs/stable/getting_started/comparison/comparison_with_sql.html#top-n-rows-per-group
    # Also, filter by dose form and ingredient term type (if appliable)
    rxcui_ndc_df = get_rxcui_ndc_df(rxcui_product_df, module_name, settings)

    #Join MEPS data with rxcui_ndc_df
    meps_rxcui_ndc_df = get_meps_rxcui_ndc_df(rxcui_ndc_df, module_name, settings)

    #Generate distribution CSVs
    dcp_demographictotal_ingred_df, dcp_demographictotal_prod_df = generate_module_csv(meps_rxcui_ndc_df, module_name, settings)

    #Generate JSON
    generate_module_json(meps_rxcui_ndc_df, dcp_demographictotal_ingred_df, dcp_demographictotal_prod_df, module_name, settings)


def main():
    # Main command and child command setup
    parser = argparse.ArgumentParser(
        description='Medication Diversification Tool for Synthea'
    )

    subparsers = parser.add_subparsers(
        title='Commands',
        metavar='',
    )

    # Init command parsers

    init_parser = subparsers.add_parser(
        'init',
        description='Download MEPS, RxNorm data and set up the database',
        help='Initialize MDT DB'
    )
    init_parser.set_defaults(func=init_db)

    # Module ommand parsers

    module_parser = subparsers.add_parser(
        'module',
        description='Module-specific commands',
        help='Module-specific commands'
    )
    module_parser.add_argument(
        '--module-name',
        '-n',
        help='Specific name of module',
    )

    module_subparser = module_parser.add_subparsers(
        title='Commands',
        metavar='',
        dest='module_commands'
    )

    create_parser = module_subparser.add_parser(
        'create',
        description='Create template module directory',
        help='Create template module directory'
    )
    create_parser.set_defaults(func=module_create)

    build_parser = module_subparser.add_parser(
        'build',
        description='Build Synthea module',
        help='Build Synthea module'
    )

    build_parser.set_defaults(func=module_build)

    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    try:
        args.func(args)
    except AttributeError:
        for key, _ in vars(args).items():
            if key == 'module_commands':
                module_parser.print_help()
