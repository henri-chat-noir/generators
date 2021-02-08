def country_alpha2(country):
    """
    Convenience function for converting country name into alpha 2 codes
    """
    if not isinstance(country, str):
        return ''
    try:
        return pyc.countries.get(name=country).alpha_2
    except KeyError:
        return ''

def to_categorical_columns(df):
    """
    Helper function to set datatype of columns 'Fueltype', 'Country', 'Set',
    'File', 'Technology' to categorical.
    """
    cols = ['Fueltype', 'Country', 'Set', 'File']
    cats = {'Fueltype': get_config()['target_fueltypes'],
            'Country': get_config()['target_countries'],
            'Set': get_config()['target_sets']}
    return df.assign(**{c: df[c].astype('category') for c in cols})\
             .assign(**{c: lambda df: df[c].cat.set_categories(v)
                        for c, v in cats.items()})


def select_by_projectID(df, projectID, dataset_name=None):
    """
    Convenience function to select data by its projectID
    """
    # df = get_obj_if_Acc(df)

    if isinstance(df.projectID.iloc[0], str):
        return df.query("projectID == @projectID")
    else:
        return df[df['projectID'].apply(lambda x:
                                        projectID in sum(x.values(), []))]

def update_saved_matches_for_(name):
    """
    Update your saved matched for a single source. This is very helpful if you
    modified/updated a data source and do not want to run the whole matching
    again.

    Example
    -------

    Assume data source 'ESE' changed a little:

    >>> pm.utils.update_saved_matches_for_('ESE')
    ... <Wait for the update> ...
    >>> pm.collection.matched_data(update=True)

    Now the matched_data is updated with the modified version of ESE.
    """
    from collection import collect
    from matching import compare_two_datasets
    df = collect(name, use_saved_aggregation=False)
    dfs = [ds for ds in get_config()['matching_sources'] if ds != name]
    for to_match in dfs:
        compare_two_datasets([collect(to_match), df], [to_match, name])

def fun(f, q_in, q_out):
    """
    Helper function for multiprocessing in classes/functions
    """
    while True:
        i, x = q_in.get()
        if i is None:
            break
        q_out.put((i, f(x)))


def to_list_if_other(obj):
    """
    Convenience function to ensure list-like output
    """
    if not isinstance(obj, list):
        return [obj]
    else:
        return obj

def convert_country_to_alpha2(df):
    # df = get_obj_if_Acc(df)
    alpha2 = df.Country.map(country_map.set_index('name')['alpha_2'])\
               .fillna(country_map.dropna(subset=['official_name'])
                       .set_index('official_name')['alpha_2'])
    return df.assign(Country=alpha2)

def breakdown_matches(df):
    """
    Function to inspect grouped and matched entries of a matched
    dataframe. Breaks down to all ingoing data on detailed level.

    Parameters
    ----------
    df : pd.DataFrame
        Matched data with not empty projectID-column. Keys of projectID must
        be specified in powerplantmatching.data.data_config
    """
    # df = get_obj_if_Acc(df)

    import data
    assert('projectID' in df)
    if isinstance(df.projectID.iloc[0], list):
        sources = [df.powerplant.get_name()]
        single_source_b = True
    else:
        sources = df.projectID.apply(list).explode().unique()
        single_source_b = False
    sources = pd.concat(
        [getattr(data, s)().set_index('projectID')
         for s in sources], sort=False)
    if df.index.nlevels > 1:
        stackedIDs = (df['projectID'].stack()
                      .apply(pd.Series).stack()
                      .dropna())
    elif single_source_b:
        stackedIDs = (df['projectID']
                      .apply(pd.Series).stack())
    else:
        stackedIDs = (df['projectID']
                      .apply(pd.Series).stack()
                      .apply(pd.Series).stack()
                      .dropna())
    return (sources
            .reindex(stackedIDs)
            .set_axis(stackedIDs.to_frame('projectID')
                      .set_index('projectID', append=True).droplevel(-2).index,
                      inplace=False)
            .rename_axis(index=['id', 'source', 'projectID']))

def restore_blocks(df, mode=2, config=None):
    """
    Restore blocks of powerplants from a matched dataframe.

    This function breaks down all matches. For each match separately it selects
    blocks from only one input data source.
    For this selection the following modi are available:

        1. Select the source with most number of blocks in the match

        2. Select the source with the highest reliability score

    Parameters
    ----------
    df : pd.DataFrame
        Matched data with not empty projectID-column. Keys of projectID must
        be specified in powerplantmatching.data.data_config
    """
    from data import OPSD
    # df = get_obj_if_Acc(df)
    assert('projectID' in df)

    config = get_config() if config is None else config

    bd = breakdown_matches(df)
    if mode == 1:
        block_map = (bd.reset_index(['source'])['source'].groupby(level='id')
                     .agg(lambda x: pd.Series(x).mode()[0]))
        blocks_i = pd.MultiIndex.from_frame(block_map.reset_index())
        res = bd.reset_index('projectID').loc[blocks_i].set_index('projectID',
                                                                  append=True)
    elif mode == 2:
        sources = df.projectID.apply(list).explode().unique()
        rel_scores = pd.Series({s: config[s]['reliability_score']
                                for s in sources})\
                       .sort_values(ascending=False)
        res = pd.DataFrame().rename_axis(index='id')
        for s in rel_scores.index:
            subset = bd.reindex(index=[s], level='source')
            subset_i = subset.index.unique(
                'id').difference(res.index.unique('id'))
            res = pd.concat([res, subset.reindex(index=subset_i, level='id')])
    else:
        raise ValueError(f'Given `mode` must be either 1 or 2 but is: {mode}')

    res = res.sort_index(level='id').reset_index(level=[0, 1])

    # Now append Block information from OPSD German list:
    df_blocks = (OPSD(rawDE_withBlocks=True)
                 .rename(columns={'name_bnetza': 'Name'}))['Name']
    res.update(df_blocks)
    return res
