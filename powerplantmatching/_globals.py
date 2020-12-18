from os import path
import pandas as pd
import yaml

NEW_ROUTE = {'ENTSOE', 'GEO'}

def _package_data(filename):
        path_str = path.join(PACKAGE_CONFIG['repo_data_dir'], filename)
        return path_str

def _get_config(filename=None, **overrides):
    """
    Import the default configuration file and update custom settings.

    Parameters
    ----------
    filename : str, optional
        DESCRIPTION. The default is None.
    **overrides : dict
        DESCRIPTION.

    Returns
    -------
    config : dict
        The configuration dictionary
    """
    
    package_config_spec = _package_data('config.yaml')
    custom_config = filename if filename else _package_data('custom.yaml')

    with open(package_config_spec) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    if path.exists(custom_config):
        with open(custom_config) as f:
            config.update(yaml.load(f, Loader=yaml.FullLoader))

    config.update(overrides)

    if len(dict(**overrides)) == 0:
        config['hash'] = 'default'

    else:
        stop = True
        from base64 import encodestring
        from hashlib import sha1
        from six.moves import cPickle
        sha1digest = sha1(cPickle.dumps(overrides)).digest()
        config['hash'] = encodestring(sha1digest).decode('ascii')[2:12]

    return config

def _set_path(filename, sub_dir=None):

    if sub_dir is None:
        path_str = path.join(PACKAGE_CONFIG['data_dir'], filename) 
    else:
        path_str = path.join(PACKAGE_CONFIG['data_dir'], sub_dir, filename) 

    return path_str

# Set-up dictionary for folder locations
ppm_data_dir = r"C:\Google Drive\0 MVR_Platform_Data\ppm_data"

repo_data_dir = path.join(path.dirname(__file__), 'package_data')

SUB_LAND = '01_landing'
SUB_CLEAN = '02_cleaned'
SUB_TAG = '03_tag_cliques'
SUB_MERGE = '04_merged'
# Placeholder for dealing with cross-table UNIT, rather than plant, matching / linking
SUB_GROUP = '06_grouped'
SUB_LINK = '08_linked'
SUB_OUT = '09_output'

PACKAGE_CONFIG = {'repo_data_dir': repo_data_dir,
                  'custom_config': path.join(repo_data_dir, '.powerplantmatching_config.yaml'),
                  'data_dir': ppm_data_dir,
                  'downloaders': {}}

CONFIG = _get_config()

DATASET_LABELS = [label for label in CONFIG['matching_sources']]

# Deal with the case that only one dataset is requested
# if isinstance(dataset_ids, str):
#   return df_by_name(datasets)
DATASET_LABELS.sort()
    
COUNTRY_MAP = pd.read_csv(_package_data('country_codes.csv'))\
                .replace({'name': {'Czechia': 'Czech Republic'}})
