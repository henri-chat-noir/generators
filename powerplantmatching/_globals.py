from os import path
import yaml

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

# Set-up dictionary for folder locations
ppm_data_dir = r"C:\Google Drive\0 MVR_Platform_Data\ppm_data"

repo_data_dir = path.join(path.dirname(__file__), 'package_data')

DATA_SUB_IN = 'landing'
DATA_SUB_WORK = 'working'
DATA_SUB_OUT = 'output'

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
    