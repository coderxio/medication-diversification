from pathlib import Path
from ruamel.yaml import YAML

yaml = YAML()


MDT_SETTINGS = '''\
# Base Application settings for module generation
state_prefix: Prescribe_
ingredient_distribution_suffix: _ingredient_distribution
product_distribution_suffix: _product_distribution
'''

MODULE_SETTINGS = '''\
module:
    as_needed: false
    chronic: false
    refills: 0
rxclass:
    # Include specfific rxclass Ids
    include:
        #  - class_id: R01AD
        #    relationship: ATC
    exclude:
        #  - class_id: bla
rxcui:
    include:
        # - somecui
    exclude:
        # - somecui
ingredient_tty_filter: IN
dosage_filter:
    # - Dry Powder Inhaler
meps:
    age: ["0-3", "4-7"]
    demographic_distribution_flags:
        age: "yes"
        gender: "yes"
        state: "yes"
    year: 18
'''

config_schema = {
    'state_prefix': ((str,), ''),
    'ingredient_distribution_suffix': ((str,), ''),
    'product_distribution_suffix': ((str,), ''),
    'module': {
        'as_needed': ((bool,), ''),
        'chronic': ((bool,), ''),
        'refills': ((int,), ''),
    },
    'rxclass': {
        'include': ((list, type(None)), ''),
        'exclude': ((list, type(None)), ''),
    },
    'ingredient_tty_filter': ((str,), 'must be either IN, MIN'),
    'dosage_filter': ((list, type(None)), ''),
    'meps': {
        'age': ((list,), "['0-3']"),
        'demographic_distribution_flags': ((object,), ''),
        'year': ((int,), '')
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
