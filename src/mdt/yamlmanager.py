from pathlib import Path
from ruamel.yaml import YAML

yaml = YAML()


MDT_SETTINGS = '''\
# Base Application settings for module generation
state_prefix: Prescribe_
ingredient_distribution_suffix: _ingredient_distribution
product_distribution_suffix: _product_distribution
default_age_ranges:
    - 0-3
    - 4-7
    - 8-11
    - 12-17
    - 18-25
    - 26-35
    - 36-45
    - 46-65
    - 65-103
'''

MODULE_SETTINGS = '''\
# Settings for the Synthea module
module:
    name:                   # optional, defaults to the camelcase name of the module folder
    assign_to_attribute:    # optional, defaults to the lowercase name of the module folder
    as_needed:              # optional, defaults to false
    chronic:                # optional, defaults to false
    refills:                # optional, defaults to 0

# Settings for the RxClass search to include/exclude
# *** At least one RxClass include or RXCUI include is required ***
# NOTE: you can include/exclude multiple class_id/relationship pairs
# RxClass options - see https://mor.nlm.nih.gov/RxClass/
rxclass:
    include:
        - class_id: 
          relationship: 
    exclude:
        - class_id:
          relationship:

# Settings for individual RXCUIs to include/exclude
# *** At least one RxClass include or RXCUI include is required ***
# NOTE: you can include/exclude multiple RXCUIs
# You must enclose RXCUIs in quotes - example: '435'
# RXCUI options - see the Ingredient section in https://mor.nlm.nih.gov/RxNav/
# Dose form options - see https://www.nlm.nih.gov/research/umls/rxnorm/docs/appendix3.html
rxcui:
    include:
        - 
    exclude:
        - 
ingredient_tty_filter:      # optional, options are IN or MIN
dose_form_filter:           # optional, see dose form options above
    - 

# Settings for the MEPS population
meps:
    age_ranges:             # optional, defaults to system defaults
        -
    demographic_distribution_flags:
        age:                # optional, defaults to true
        gender:             # optional, defaults to true
        state:              # optional, defaults to true
    year:                   # optional, defaults to 18 (2018)
'''

config_schema = {
    'state_prefix': ((str,), ''),
    'ingredient_distribution_suffix': ((str,), ''),
    'product_distribution_suffix': ((str,), ''),
    'default_age_ranges': ((list), ''),
    'module': {
        'name': ((str, type(None)), ''),
        'assign_to_attribute': ((str, type(None)), ''),
        'as_needed': ((bool, type(None)), ''),
        'chronic': ((bool, type(None)), ''),
        'refills': ((int, type(None)), ''),
    },
    'rxclass': {
        'include': ((list, type(None)), ''),
        'exclude': ((list, type(None)), ''),
    },
    'rxcui': {
        'include': ((list, type(None)), ''),
        'exclude': ((list, type(None)), ''),
    },
    'ingredient_tty_filter': ((str, type(None)), 'must be either IN, MIN'),
    'dose_form_filter': ((list, type(None)), ''),
    'meps': {
        'age_ranges': ((list, type(None)), "['0-3']"),
        'demographic_distribution_flags': ((object,), ''),
        'year': ((int, type(None)), '')
    }
}


def validate_config(config, schema=config_schema):
    err = []

    for setting, attributes in schema.items():

        if isinstance(attributes, tuple):
            value_type, err_message = attributes
            if not isinstance(config[setting], value_type):
                err.append(
                    f'{setting} must be of type {value_type} {err_message}'
                )

        if isinstance(attributes, dict):
            for attribute, value in attributes.items():
                value_type, err_message = value
                if not isinstance(config[setting][attribute], value_type):
                    err.append(
                        f'{attribute} must be of type {value_type} {err_message}'
                    )
        
    if ((config['rxclass']['include'] is None
            or config['rxclass']['include'][0]['class_id'] is None) 
        and (config['rxcui']['include'] is None 
            or config['rxcui']['include'][0] is None)):
        err.append('Must have at least one RxClass include or RXCUI include.')

    sep = '\n'

    if err:
        raise ValueError(f'Config file validation error\n{sep.join(err)}')


def create_mdt_settings(path=Path.cwd()):
    settings = path / 'mdt-settings.yaml'

    if not settings.exists():
        data = yaml.load(MDT_SETTINGS)
        yaml.dump(data, settings)


def create_module_settings(module_name, path=Path.cwd()):
    module = path / module_name

    if not module.exists():
        module.mkdir(parents=True)
        data = yaml.load(MODULE_SETTINGS)
        yaml.dump(data, (module / 'settings.yaml'))

def get_settings(module_name, path=Path.cwd()):
    module_settings = path / module_name / 'settings.yaml'
    mdt_settings = path / 'mdt-settings.yaml'

    if not module_settings.exists():
        raise FileNotFoundError(f'Settings file does not exist in the {module_name} module.')
    elif not mdt_settings.exists():
        raise FileNotFoundError('MDT settings file does not exist.')

    module_data = yaml.load(module_settings)
    mdt_data = yaml.load(mdt_settings)
    settings = {**module_data, **mdt_data}

    validate_config(settings)

    return settings
