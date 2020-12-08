import yaml
import logging
from os import environ, makedirs, path

from _globals import CONFIG, PACKAGE_CONFIG, DATA_SUB_IN, DATA_SUB_OUT

# CORE FUNCTIONS
# =============
def _data_in(filename):

    path_str = path.join(PACKAGE_CONFIG['data_dir'], DATA_SUB_IN, filename) 
    return path_str

def _data_out(filename):

    path_str = path.join(PACKAGE_CONFIG['data_dir'], DATA_SUB_OUT, 'default', filename)
    return path_str

"""
def get_obj_if_Acc(obj):

    from accessor import PowerPlantAccessor

    if isinstance(obj, PowerPlantAccessor):
        return obj._obj
    else:
        return obj
"""

# INITIALIZATION CODE
# ===================

ppm_data_dir = PACKAGE_CONFIG['data_dir']
makedirs(path.join(ppm_data_dir, DATA_SUB_IN), exist_ok=True)
makedirs(path.join(ppm_data_dir, DATA_SUB_OUT), exist_ok=True)

# del _data_dir
# del _writable_dir

if not path.exists(_data_in('.')):
    makedirs(_data_in('.'))

data_out_path = _data_out('.')

if not path.isdir(data_out_path):
    makedirs( path.abspath(data_out_path) )
    makedirs( path.abspath( _data_out('matches')))
    makedirs(path.abspath(_data_out('aggregations')))
    logging.info(f"Outputs for this configuration will be saved under {data_out_path}")
            
    with open(_data_out('config.yaml'), 'w') as file:
        yaml.dump(CONFIG, file, default_flow_style=False)

# Logging: General Settings
logger = logging.getLogger(__name__)
logging.basicConfig(level=20)
logger.setLevel('INFO')

# Logging: File
logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] "
                                 "[%(levelname)-5.5s]  %(message)s")
fileHandler = logging.FileHandler(path.join(PACKAGE_CONFIG['data_dir'], 'PPM.log'))
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)
# logger.info('Initialization complete.')

del logFormatter
del fileHandler