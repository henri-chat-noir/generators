import os
import logging
import pandas as pd

from .core import _data_out
from . import data

from .utils import  (set_uncommon_fueltypes_to_other, parmap,
                    to_dict_if_string, projectID_to_dict, set_column_name)

from .heuristics import extend_by_non_matched, extend_by_VRE
from .cleaning import aggregate_units
from .matching import combine_multiple_datasets, reduce_matched_dataframe

logger = logging.getLogger(__name__)

def collect_datasets(config,
                     rebuild_collection=True,
                     use_saved_aggregation=True,
                     use_saved_matches=True,
                     custom_config={},
                     **dukeargs):

    """
    Return the collection for a given list of datasets in matched or
    reduced form.

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

    def df_by_name(name):
        conf = config[name]

        get_df = getattr(data, name)
        df = get_df(config=config, **conf.get('read_kwargs', {}))
        if not conf.get('aggregated_units', False):
            return_obj = aggregate_units(df,
                                   use_saved_aggregation=use_saved_aggregation,
                                   dataset_name=name,
                                   config=config)
        else:
            return_obj = df.assign(projectID=df.projectID.map(lambda x: [x]))


        return return_obj

    # dataset_ids = [list(to_dict_if_string(a))[0] for a in config['matching_sources']]
    dataset_ids = []
    for id_label in config['matching_sources']:
        list_elem = list( to_dict_if_string(id_label) )[0]
        dataset_ids.append(list_elem)

    # Deal with the case that only one dataset is requested
    # if isinstance(dataset_ids, str):
    #   return df_by_name(datasets)

    datasets = sorted(dataset_ids)
    
    logger.info('Collect combined dataset for {}'.format(', '.join(datasets)))
    outfn_matched = _data_out('Matched_{}.csv'
                              .format('_'.join(map(str.upper, datasets))),
                              config=config)
    outfn_reduced = _data_out('Matched_{}_reduced.csv'
                              .format('_'.join(map(str.upper, datasets))),
                              config=config)

    file_exists = os.path.exists(outfn_reduced)
    if not rebuild_collection and not file_exists:
        logger.warning(f"Forcing rebuild of working files since {outfn_reduced} is missing")
        rebuild_collection = True
        use_saved_aggregation = True

    if rebuild_collection:
        dfs = parmap(df_by_name, datasets)
        matched = combine_multiple_datasets(
            dfs, datasets, use_saved_matches=use_saved_matches, config=config,
            **dukeargs)
        (matched.assign(projectID=lambda df: df.projectID.astype(str))
                .to_csv(outfn_matched, index_label='id'))

        # Need to figure out what the reduction is all about
        reduced_df = reduce_matched_dataframe(matched, config=config)
        reduced_df.to_csv(outfn_reduced, index_label='id')
        return_df = reduced_df

    else:
        df = pd.read_csv(outfn_reduced, index_col=0)
        return_df = df.pipe(projectID_to_dict)

    return return_df

def Collection(**kwargs):
    return collect(**kwargs)

def build_plants(coll_df, config,
                 extend_by_vres=False,
                 extendby_kwargs={'use_saved_aggregation': True},
                 subsume_uncommon_fueltypes=False
                 ):
    """
    Return the full matched dataset including all data sources listed in
    config.yaml/matching_sources. The combined data is additionally extended
    by non-matched entries of sources given in
    config.yaml/fully_inculded_souces.

    Parameters
    ----------
    stored : Boolean, default True
            Whether to use the stored matched_data.csv file in data/out/default
            If False, the matched data is taken from collect() and
            extended afterwards. To update the whole matching, please set
            stored=False and update=True.
    update : Boolean, default False
            Whether to rerun the matching process.
            Overrides stored to False if True.
    update_all : Boolean, default False
            Whether to rerun the matching process and aggregation process.
            Overrides stored to False if True.
    from_url: Boolean, default False
            Whether to parse and store the already build data from the repo website.
    config : Dict, default None
            Define a configuration varying from the setting in config.yaml.
            Relevant keywords are 'matching_sources', 'fully_included_sources'.
    subsume_uncommon_fueltypes : Boolean, default False
            Whether to replace uncommon fueltype specification by 'Other'
    
    """

    # save_reduced = collection_kwargs.get('reduced', True)
    save_reduced = False
    if save_reduced:
        fn = _data_out('matched_data_red.csv')
        header = 0
    else:
        fn = _data_out('matched_data.csv')
        header = [0, 1]

    # exists_bool = os.path.exists(fn)

    if isinstance(config['fully_included_sources'], list):
        for source in config['fully_included_sources']:
            source = to_dict_if_string(source)
            name, = list(source)
            extendby_kwargs.update({'query': source[name]})
            matched = extend_by_non_matched(coll_df, name, config=config, **extendby_kwargs)

    # Drop matches between only low reliability-data, this is necessary since
    # a lot of those are decommissioned, however some countries only appear in
    # GEO and CARMA
    allowed_countries = config['CARMA_GEO_countries']
    if coll_df.columns.nlevels > 1:
        other = set(matching_sources) - set(['CARMA', 'GEO'])
        matched = (coll_df[coll_df.projectID[other].isna().all(1)
                           | coll_df.Country.GEO.isin(allowed_countries)
                           | coll_df.Country.CARMA.isin(allowed_countries)]
                   .reset_index(drop=True))
        if config['remove_missing_coords']:
            matched = (matched[matched.lat.notnull().any(1)]
                       .reset_index(drop=True))
    else:
        matched = (coll_df[coll_df.projectID.apply(lambda x: sorted(x.keys())
                                                   not in [['CARMA', 'GEO']])
                           | coll_df.Country.isin(allowed_countries)]
                   .reset_index(drop=True))

        if config['remove_missing_coords']:
            matched = matched[matched.lat.notnull()].reset_index(drop=True)
    
    # Save csv file    
    matched.to_csv(fn, index_label='id', encoding='utf-8')

    if subsume_uncommon_fueltypes:
        matched = set_uncommon_fueltypes_to_other(matched)

    return matched.pipe(set_column_name, 'Matched Data')

def extend_df(df):

    """
    extend_by_vres : Boolean, default False
            Whether extend the dataset by variable renewable energy sources
            given by powerplantmatching.data.OPSD_VRE()
    extendby_kwargs : Dict, default {'use_saved_aggregation': True}
            Dict of keywordarguments passed to powerplantmatchting.
            heuristics.extend_by_non_matched
    """

    # if stored and exists_bool:

    df = (pd.read_csv(fn, index_col=0, header=header)
            .pipe(projectID_to_dict)
            .pipe(set_column_name, 'Matched Data'))

    if extend_by_vres:
        return df.pipe(extend_by_VRE, config=config,
                        base_year=config['opsd_vres_base_year'])

    if extend_by_vres:
        matched = extend_by_VRE(matched, config=config,
                                base_year=config['opsd_vres_base_year'])

    return df