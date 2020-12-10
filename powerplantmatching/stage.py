import pandas as pd
import logging

from _globals import CONFIG, DATASET_LABELS, TEST_DS
from core import _data_in, _data_out
from utils import parmap
from augment import aggregate_units

import data
logger = logging.getLogger(__name__)

def load_dataframes():

    def load_df(ds_name):
  
        ds_config = CONFIG[ds_name]
        if ds_name in TEST_DS:
            ds_spec = _data_in(ds_config['fn'])
            df = pd.read_csv(ds_spec)
        
        else:
            df = None
            get_df = getattr(data, ds_name)
            df = get_df(df=df, config=CONFIG, **ds_config.get('read_kwargs', {}))
            print(df.columns)
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
        
        if ds_name in TEST_DS:
            ds_config = CONFIG[ds_name]
            get_df = getattr(data, ds_name)
            stop = True
            df = get_df(in_df=df, config=CONFIG, **ds_config.get('read_kwargs', {}))
            print(df.columns)
            stop = True
            if not ds_config.get('aggregated_units', False):
                df = aggregate_units(df, use_saved_aggregation=True, dataset_name=ds_name, config=CONFIG)
            else:
                df = df.assign(projectID=df.projectID.map(lambda x: [x]))

        out_dfs.append(df)

    return out_dfs


def consolidate_dataframes():

    pass
    return