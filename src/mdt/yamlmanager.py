MDT_SETTINGS = '''\
# Base Application settings for module generation
state_prefix: Prescribe_
ingredient_distribution_suffix: _product_distribution
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
    'module': {
        'as_needed': ((bool,), ''),
        'chronic': ((bool,), ''),
        'refills': ((int,), ''),
    },
    'rxclass': {
        'include': ((list, type(None)), ''),
        'exclude': ((list, type(None)), ''),
    },
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
