from os import path
import pandas as pd
import yaml

import ppm.file_handling as fh

ALL_DATASETS = {'CARMA', 'ENTSOE', 'ESE', 'GEO', 'GPD', 'JRC', 'OPSD_DE', 'OPSD_EU'}
NOW_GONE_IDS = ['50WP00000000707U', 'H70', 'H199', 'H223', 'H573', 'H1495', 'H1554', 'H2499', 'H2501', 'H37', 'H2877', 'H2565', 'H3189']

MATCH_COLS = ["GroupKey", "Status", "PlantName", 
            	"OrigName", "Capacity", "Country", "Dataset", "EIC",
                 "Fueltype", "Set", "Technology", "lat", "lon", "UnitName"
                ]

RUN_SEARCH_DF = pd.DataFrame()

# Set-up dictionary for folder locations
ppm_data_dir = r"C:\Google Drive\0 MVR_Platform_Data\ppm_data"
package_data_dir = path.join(path.dirname(__file__), 'package_data')
package_diag_dir = path.join(path.dirname(__file__), 'package_diag')
ref_data_dir = path.join(path.dirname(__file__), 'ref_data')

PACKAGE_CONFIG = {'package_data_dir': package_data_dir,
                  'package_diag_dir': package_diag_dir,
                  'ref_data_dir': ref_data_dir,
                  'custom_config': path.join(package_data_dir, '.powerplantmatching_config.yaml'),
                  'data_dir': ppm_data_dir,
                  'downloaders': {}}

def package_data(filename):
        path_str = path.join(PACKAGE_CONFIG['package_data_dir'], filename)
        return path_str

def package_diag(filename):
        path_str = path.join(PACKAGE_CONFIG['package_diag_dir'], filename)
        return path_str

def ref_data(filename):
        path_str = path.join(PACKAGE_CONFIG['ref_data_dir'], filename)
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
    
    package_config_spec = package_data('config.yaml')
    custom_config = filename if filename else package_data('custom.yaml')

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

def set_path(filename, sub_dir=None):

    if sub_dir is None:
        path_str = path.join(PACKAGE_CONFIG['data_dir'], filename) 
    else:
        path_str = path.join(PACKAGE_CONFIG['data_dir'], sub_dir, filename) 

    return path_str

def set_diag_path(fn, sub_dir, sub_sub_dir='0_diagnostics'):

    path_str = path.join(PACKAGE_CONFIG['data_dir'], sub_dir, sub_sub_dir, fn) 

    return path_str

SUB_DEBUG = '00_debug'
SUB_LAND = '01_landing'
SUB_CLEAN = '02_cleaned'
SUB_FILTER = '03_filtered'
SUB_GOOGLE = '04_google_api'
SUB_TAG = '05_tag_cliques'
SUB_MERGE = '06_merged'
# Placeholder for dealing with cross-table UNIT, rather than plant, matching / linking
SUB_GROUP = '07_grouped'
SUB_LINK = '08_linked'
SUB_OUT = '09_output'
SUB_DIAG = '99_diagnostics'

# FILE TAGS (used for interchange between processing stages)
# ==========================================================
TAG_CLEAN = "clean"
TAG_FILTER = "filter"
TAG_GOOGLE = "google"

CONFIG = _get_config()

DATASET_LABELS = [label for label in CONFIG['matching_sources']]

# Deal with the case that only one dataset is requested
# if isinstance(dataset_ids, str):
#   return df_by_name(datasets)
DATASET_LABELS.sort()
    
COUNTRY_MAP = pd.read_csv(ref_data('country_codes.csv'))\
                .replace({'name': {'Czechia': 'Czech Republic'}})

# new_id_spec = package_data("new_ids.csv")
# NEW_IDS_DF = pd.read_csv(new_id_spec, index_col="projectID")