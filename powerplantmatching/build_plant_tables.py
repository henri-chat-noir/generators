import os
import logging
import pandas as pd

from _globals import CONFIG, DATASET_LABELS
from core import _data_out

from utils import set_uncommon_fueltypes_to_other, projectID_to_dict, set_column_name, get_name

from heuristics import extend_by_non_matched, extend_by_VRE
import match

logger = logging.getLogger(__name__)

def build_plant_df_alldata(dfs, custom_config={}, **dukeargs):

    """
    Return the collection for a given list of datasets in matched or reduced form.

    Parameters
    ----------
    datasets : list or str
        list containing the dataset identifiers as str, or single str
    update : bool
        Do an horizontal update (True) or read from the cache file (False)
    use_saved_aggregation : bool
        Aggregate units based on cached aggregation group files (True)
        or to do an vertical update (False)

    use_saved_matches : bool
        Match datasets based on cached matched pair files (True)
        or to do an horizontal matching (False)
    reduced : bool
        Switch as to return the reduced (True) or matched (False) dataset.
    custom_config : dict
        Updates to the data_config dict from data module
    **dukeargs : keyword-args for duke
    """

    plants_alldata_df = match.combine_multiple_datasets( dfs, use_saved_matches=True, **dukeargs)
    
    # This type set should already be part of tidying / normalization, note at this stage
    plants_alldata_df.assign( projectID=lambda df: df.projectID.astype(str) )
    
    datasets_filetag = '_'.join(DATASET_LABELS)
    outfn_alldata = _data_out(f'plants_list-ALLDATA_{datasets_filetag}.csv')
    plants_alldata_df.to_csv(outfn_alldata, index_label='id')

    return plants_alldata_df

def build_plant_df_reduced(plants_alldata_df):

    data_set_str = '_'.join(DATASET_LABELS)
    outfn_reduced = _data_out(f'plants_list_{data_set_str}.csv')

    # logger.info('Collect combined dataset for {}'.format(', '.join(datasets)))
    
    plants_reduced_df = match.reduce_matched_dataframe(plants_alldata_df)
    plants_reduced_df.to_csv(outfn_reduced, index_label='id')

    return plants_reduced_df