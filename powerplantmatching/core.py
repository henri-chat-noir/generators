import logging
from os import environ, makedirs, path

# CORE FUNCTIONS
# =============
def _package_data(filename):
    return path.join(package_config['repo_data_dir'], filename)

def _data_in(filename):
    return path.join(package_config['data_dir'], 'data', 'in', filename)

def _data_out(filename, config=None):

    if config is None:
        path_str = path.join(package_config['data_dir'], 'data', 'out', 'default', filename)
    else:
        path_str = path.join(package_config['data_dir'], 'data', 'out', config['hash'], filename)

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
    
    import yaml
    from logging import info

    package_config = _package_data('config.yaml')
    custom_config = filename if filename else _package_data('custom.yaml')

    with open(package_config) as f:
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

    if not path.isdir(_data_out('.', config=config)):
        makedirs(path.abspath(_data_out('.', config=config)))
        makedirs(path.abspath(_data_out('matches', config=config)))
        makedirs(path.abspath(_data_out('aggregations', config=config)))
        info('Outputs for this configuration will be saved under {}'
             .format(path.abspath(_data_out('.', config=config))))
        
        with open(_data_out('config.yaml', config=config), 'w') as file:
            yaml.dump(config, file, default_flow_style=False)
    
    return config

def get_obj_if_Acc(obj):

    from .accessor import PowerPlantAccessor

    if isinstance(obj, PowerPlantAccessor):
        return obj._obj
    else:
        return obj


# INITIALIZATION CODE
# ===================

# data_root_dir = path.expanduser('~')
# _writable_dir = path.join(data_root_dir, '.local', 'share')
# _data_dir = path.join( environ.get("XDG_DATA_HOME", environ.get("APPDATA", _writable_dir)), 'powerplantmatching')
# Creates _data_dir as r"C:\\Users\\John\\AppData\\Roaming\\powerplantmatching"

# Set-up dictionary for folder locations
data_parent = r"C:\henri-chat-noir\generators\powerplantmatching"
package_config = {'custom_config': path.join(data_parent, '.powerplantmatching_config.yaml'),
                  'data_dir': data_parent,
                  'repo_data_dir': path.join(path.dirname(__file__), 'package_data'),
                  'downloaders': {}}

makedirs(path.join(package_config['data_dir'], 'data', 'in'), exist_ok=True)
makedirs(path.join(package_config['data_dir'], 'data', 'out'), exist_ok=True)

# del _data_dir
# del _writable_dir

if not path.exists(_data_in('.')):
    makedirs(_data_in('.'))

# Logging: General Settings
logger = logging.getLogger(__name__)
logging.basicConfig(level=20)
logger.setLevel('INFO')

# Logging: File
logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] "
                                 "[%(levelname)-5.5s]  %(message)s")
fileHandler = logging.FileHandler(path.join(package_config['data_dir'], 'PPM.log'))
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)
# logger.info('Initialization complete.')

del logFormatter
del fileHandler