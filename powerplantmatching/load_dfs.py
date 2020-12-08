import logging
from _globals import CONFIG, DATASET_LABELS
from core import _data_out
from utils import parmap
from cleaning import aggregate_units

import data
logger = logging.getLogger(__name__)

def load_dataframes():

    def df_by_name(name):
        conf = CONFIG[name]

        get_df = getattr(data, name)
        df = get_df( config=CONFIG, **conf.get('read_kwargs', {}))
        if not conf.get('aggregated_units', False):
            return_obj = aggregate_units(df, use_saved_aggregation=True, dataset_name=name, config=CONFIG)
        else:
            return_obj = df.assign(projectID=df.projectID.map(lambda x: [x]))

        return return_obj

    logger.info(f"Loading csvs to form combined dataset for {DATASET_LABELS}")
    dfs = parmap(df_by_name, DATASET_LABELS)

    return dfs