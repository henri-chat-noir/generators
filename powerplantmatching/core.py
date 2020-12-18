import yaml
import logging
from os import environ, makedirs, path

from _globals import _set_path, CONFIG, PACKAGE_CONFIG, SUB_LAND, SUB_OUT

"""
def _data_in(filename):
    path_str = path.join(PACKAGE_CONFIG['data_dir'], DATA_SUB_IN, filename) 
    return path_str

def _data_work(filename):
    path_str = path.join(PACKAGE_CONFIG['data_dir'], DATA_SUB_WORK, filename) 
    return path_str

def _data_out(filename):
    path_str = path.join(PACKAGE_CONFIG['data_dir'], DATA_SUB_OUT, 'default', filename)
    return path_str
"""


# CORE FUNCTIONS
# =============


# INITIALIZATION CODE
# ===================

ppm_data_dir = PACKAGE_CONFIG['data_dir']
makedirs(path.join(ppm_data_dir, SUB_LAND), exist_ok=True)
makedirs(path.join(ppm_data_dir, SUB_OUT), exist_ok=True)

# del _data_dir
# del _writable_dir

if not path.exists(_set_path('.', SUB_LAND)):
    makedirs(_set_path('.', SUB_LAND))

data_out_path = _set_path('.', SUB_OUT)

if not path.isdir(data_out_path):
    makedirs( path.abspath(data_out_path) )
    makedirs( path.abspath( _set_path('matches', SUB_OUT)))
    makedirs(path.abspath(_set_path('aggregations', SUB_OUT)))
    logging.info(f"Outputs for this configuration will be saved under {data_out_path}")
            
    with open(_set_path('config.yaml', SUB_OUT), 'w') as file:
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