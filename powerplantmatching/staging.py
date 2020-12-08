import pandas as pd
import logging

from _globals import CONFIG, DATASET_LABELS
from core import _data_in, _data_out
from utils import parmap
from cleaning import aggregate_units

import data
logger = logging.getLogger(__name__)

def download_remote(ds_name, parse_func=None, **kwargs):
    
    ds_config = CONFIG[name]
    local_path = _data_in(df_config['fn'])
    
    if parse_func is None:
        ds_url = ds_config['url']
        logger.info(f'Retrieving data for {ds_name} from {df_url}')
        data = pd.read_csv(df_config['url'], **kwargs)

    else:
        data = parse_func()
        data.to_csv(path)

    return data


def load_dataframes():

    def load_df(ds_name):
  
        ds_config = CONFIG[ds_name]
        if ds_name in {"ENTSOE"}:
            ds_spec = _data_in(ds_config['fn'])
            df = pd.read_csv(ds_spec)
        
        else:
            df = None
            get_df = getattr(data, ds_name)
            df = get_df(df=df, config=CONFIG, **ds_config.get('read_kwargs', {}))
            if not ds_config.get('aggregated_units', False):
                df = aggregate_units(df, use_saved_aggregation=True, dataset_name=ds_name, config=CONFIG)
            else:
                df = df.assign(projectID=df.projectID.map(lambda x: [x]))

        df.name = ds_name
        return df

    dfs = []
    # dfs = parmap(df_by_name, DATASET_LABELS)
    for ds_name in DATASET_LABELS:
        logger.info(f"Loading csv to form dataframe {ds_name}")
        df = load_df(ds_name)
        dfs.append(df)

    return dfs

def tidy_dataframes(in_dfs):

    out_dfs = []
    for df in in_dfs:
        
        try:
            ds_name = df.name
        except:
            ds_name = None
        
        if ds_name in {'ENTSOE'}:
            ds_config = CONFIG[ds_name]
            get_df = getattr(data, ds_name)
            df = get_df(df=df, config=CONFIG, **ds_config.get('read_kwargs', {}))
            
            if not ds_config.get('aggregated_units', False):
                df = aggregate_units(df, use_saved_aggregation=True, dataset_name=ds_name, config=CONFIG)
            else:
                df = df.assign(projectID=df.projectID.map(lambda x: [x]))

        out_dfs.append(df)

    return out_dfs


"""
    Importer for the list of installed generators provided by the ENTSO-E
    Trasparency Project. Geographical information is not given.
    If update=True, the dataset is parsed through a request to
    'https://transparency.entsoe.eu/generation/r2/\
    installedCapacityPerProductionUnit/show',
    Internet connection requiered. If raw=True, the same request is done, but
    the unprocessed data is returned.

    Note: For obtaining a security token refer to section 2 of the
    RESTful API documentation of the ENTSOE-E Transparency platform
    https://transparency.entsoe.eu/content/static_content/Static%20content/
    web%20api/Guide.html#_authentication_and_authorisation. Please save the
    token in your config.yaml file (key 'entsoe_token').
    
    update : Boolean, Default False
        Whether to update the database through a request to the ENTSO-E
        transparency plattform
    raw : Boolean, Default False
        Whether to return the raw data, obtained from the request to
        the ENTSO-E transparency platform
    entsoe_token: String
        Security token of the ENTSO-E Transparency platform
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()

"""


def consolidate_dataframes():



    pass
    return