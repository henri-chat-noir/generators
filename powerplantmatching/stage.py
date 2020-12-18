import pandas as pd
import logging

from _globals import _set_path, CONFIG, DATASET_LABELS, NEW_ROUTE, SUB_LAND, SUB_CLEAN, SUB_TAG, SUB_GROUP
from utils import parmap
from augment import aggregate_units

import data
logger = logging.getLogger(__name__)

def load_dataframes():

    def load_df(ds_name):
  
        ds_config = CONFIG[ds_name]
        if ds_name in NEW_ROUTE:
            ds_spec = _set_path(ds_config['fn'], SUB_LAND)
            df = pd.read_csv(ds_spec)
            df.columns.name = ds_name        

        else:
            df = None
            get_df = getattr(data, ds_name)
            df = get_df(df=df, **ds_config.get('read_kwargs', {}))
            df.columns.name = ds_name

            if ds_config.get('aggregated_units', False):
                df = df.assign(projectID=df.projectID.map(lambda x: [x]))
            else:
                df = aggregate_units(df, ds_name=ds_name, use_saved_dukemap=True)
            
        return df

    dfs = []
    # dfs = parmap(df_by_name, DATASET_LABELS)
    for ds_name in DATASET_LABELS:
        logger.info(f"Loading csv to form dataframe {ds_name}")
        df = load_df(ds_name)
        print(df.columns.name)
        dfs.append(df)

    return dfs

def clean_dataframes(in_dfs):

    out_dfs = []
    for df in in_dfs:
        ds_name = df.columns.name
        if ds_name in NEW_ROUTE:
            ds_config = CONFIG[ds_name]
            clean_df_func = getattr(data, ds_name)
            df = clean_df_func(in_df=df, **ds_config.get('read_kwargs', {}))

            file_spec = _set_path(f"{ds_name}_clean.csv", SUB_CLEAN)
            df.to_csv(file_spec, header=True)          

        file_spec = _set_path(f"{ds_name}_clean.csv", SUB_CLEAN)
        df.to_csv(file_spec, header=True)          

        out_dfs.append(df)

    return out_dfs

def group_units(in_dfs):

    out_dfs = []
    for df in in_dfs:
        ds_name = df.columns.name
    
        if ds_name in NEW_ROUTE:
            ds_config = CONFIG[ds_name]
            if ds_config.get('aggregated_units', False):
                df = df.assign(projectID=df.projectID.map(lambda x: [x]))
            else:
                df = aggregate_units(df, use_saved_dukemap=True, ds_name=ds_name)

        file_spec = _set_path(f"{ds_name}_grouped.csv", SUB_GROUP)
        df.to_csv(file_spec, header=True)          

        out_dfs.append(df)

    return out_dfs