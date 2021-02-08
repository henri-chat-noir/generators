"""
Functions for linking and combining different datasets
"""

import os.path
import pandas as pd
import numpy as np
from itertools import combinations
import logging

from globals import CONFIG, DATASET_LABELS, SUB_LINK

from utils import parmap
from duke import duke_link
from cleaning_functions import clean_technology

logger = logging.getLogger(__name__)

def best_matches(links):
    """
    Subsequent to duke() with singlematch=True. Returns reduced list of
    matches on the base of the highest score for each duplicated entry.

    Parameters
    ----------
    links : pd.DataFrame
        Links as returned by duke
    """
    labels = links.columns.difference({'scores'})

    links = links.groupby(links.iloc[:, 1], as_index=False, sort=False)
    links = links.apply(lambda x: x.loc[x.scores.idxmax(), labels])

    return links

def compare_two_datasets(df_pair, label_pair, use_saved_matches=False, country_wise=True):
    """
    Duke-based horizontal match of two databases. Returns the matched
    dataframe including only the matched entries in a multi-indexed
    pandas.Dataframe. Compares all properties of the given columns
    ['Name','Fueltype', 'Technology', 'Country',
    'Capacity','lat', 'lon'] in order to determine the same
    powerplant in different two datasets. The match is in one-to-one
    mode, that is every entry of the initial databases has maximally
    one link in order to obtain unique entries in the resulting
    dataframe.  Attention: When aborting this command, the duke
    process will still continue in the background, wait until the
    process is finished before restarting.

    Parameters
    ----------
    dfs : list of pandas.Dataframe to use for the matching
    
    """
    
    pair = np.sort(label_pair)

    pair_match_spec = f"matches_{pair[0]}_{pair[1]}.csv"
    saving_path = _set_path(pair_match_spec, SUB_LINK)
    if use_saved_matches:
        if os.path.exists(saving_path):
            logger.info(f"Reading saved matches for df_pair {pair[0]} and {pair[1]}")
            return pd.read_csv(saving_path, index_col=0)

        else:
            logger.warning(f"Non-existing saved matches for dataset {pair[0]}, {pair[1]}."
                           "continuing by matching again . . .")

    def country_link(df_pair, country):

        # country_selector for both dataframes
        sel_country_b = [df['Country'] == country for df in df_pair]

        # only append if country appears in both dataframse
        if all(sel.any() for sel in sel_country_b):
            datasets = [ df[sel] for df, sel in zip(df_pair, sel_country_b)]
            out_df = duke_link(datasets[0], datasets[1], country=country)

        else:
            out_df = pd.DataFrame()

        return out_df

    if country_wise:
        countries = CONFIG['target_countries']
        links = pd.DataFrame()

        # links = pd.concat([country_link(dfs, c) for c in countries])
        for country in countries:
            df = country_link(df_pair, country)
            links = links.append(df)

    else:
        links = duke_link(df_pair, labels=label_pair)

    matches = best_matches(links)
    matches.to_csv(saving_path)

    return matches

def cross_matches(sets_of_pairs, df_labels):
    """
    Combines multiple sets of pairs and returns one consistent
    dataframe. Identifiers of two datasets can appear in one row even
    though they did not match directly but indirectly through a
    connecting identifier of another database.

    Parameters
    ----------
    sets_of_pairs : list
        list of pd.Dataframe's containing only the matches (without
        scores), obtained from the linkfile (duke() and
        best_matches())
    df_labels : list of strings
        list of names of the databases, used for specifying the order
        of the output

    """
    m_all = sets_of_pairs
    matches = pd.DataFrame(columns=df_labels)
    for i in df_labels:
        base = [m.set_index(i) for m in m_all if i in m]
        match_base = pd.concat(base, axis=1).reset_index()
        matches = pd.concat([matches, match_base], sort=True)

    matches = matches.drop_duplicates().reset_index(drop=True)
    for i in df_labels:
        matches = pd.concat([
            matches.groupby(i, as_index=False, sort=False)
                   .apply(lambda x: x.loc[x.isnull().sum(axis=1).idxmin()]),
            matches[matches[i].isnull()]
        ]).reset_index(drop=True)

    return (matches
            .assign(length=matches.notna().sum(axis=1))
            .sort_values(by='length', ascending=False)
            .reset_index(drop=True)
            .drop('length', axis=1)
            .reindex(columns=df_labels))

def link_multiple_datasets(datasets, use_saved_matches=True):
    """
    Duke-based horizontal match of multiple databases. Returns the
    matching indices of the datasets. Compares all properties of the
    given columns ['Name','Fueltype', 'Technology', 'Country',
    'Capacity','lat', 'lon'] in order to determine the same
    powerplant in different datasets. The match is in one-to-one mode,
    that is every entry of the initial databases has maximally one
    link to the other database.  This leads to unique entries in the
    resulting dataframe.

    Parameters
    ----------
    datasets : list of pandas.Dataframe or strings
        dataframes or csv-files to use for the matching
    labels : list of strings
        Names of the databases in alphabetical order and corresponding
        order to the datasets
    """
    
    def comp_dfs(dfs_lbs):
        print()
        logger.info('Comparing {0} with {1}'.format(*dfs_lbs[2:]))

        return compare_two_datasets(dfs_lbs[:2], dfs_lbs[2:], use_saved_matches=use_saved_matches)
    
    # Returns list of tuples (0, 1), (0, 2), etc.
    combs = list(combinations(range(len(DATASET_LABELS)), 2)) 

    mapargs = [[datasets[c], datasets[d], DATASET_LABELS[c], DATASET_LABELS[d]] for c, d in combs]
    all_matches = parmap(comp_dfs, mapargs)

    return cross_matches(all_matches, DATASET_LABELS)

def combine_multiple_datasets(dfs, use_saved_matches=True):
    """
    Duke-based horizontal match of multiple databases. Returns the
    matched dataframe including only the matched entries in a
    multi-indexed pandas.Dataframe. Compares all properties of the
    given columns ['Name','Fueltype', 'Technology', 'Country',
    'Capacity','lat', 'lon'] in order to determine the same
    powerplant in different datasets. The match is in one-to-one mode,
    that is every entry of the initial databases has maximally one
    link to the other database.  This leads to unique entries in the
    resulting dataframe.

    Parameters
    ----------
    datasets : list of pandas.Dataframes to use for the matching
    labels : list of strings
        Names of the databases in alphabetical order and corresponding
        order to the datasets
    """
    
    def combined_dataframe(cross_matches, dfs):
        """
        Use this function to create a matched dataframe on base of the
        cross matches and a list of the databases. Always order the
        database alphabetically.

        Parameters
        ----------
        cross_matches : pandas.Dataframe of the matching indexes of
            the databases, created with powerplant_collection.cross_matches()
        dfs : list of pandas.Dataframes in the same order as in cross_matches
        """
        
        # datasets = list(map(read_csv_if_string, datasets))

        for i, df in enumerate(dfs):
            dfs[i] = (df
                           .reindex(cross_matches.iloc[:, i])
                           .reset_index(drop=True))
        return (pd.concat(dfs, axis=1,
                          keys=cross_matches.columns.tolist())
                .reorder_levels([1, 0], axis=1)
                .reindex(columns=CONFIG['target_columns'], level=0)
                .reset_index(drop=True))

    crossmatches = link_multiple_datasets(dfs, use_saved_matches=use_saved_matches)

    return (combined_dataframe(crossmatches, dfs)
            .reindex(columns=CONFIG['target_columns'], level=0))

def reduce_matched_dataframe(df, show_orig_names=False):
    """
    Reduce a matched dataframe to a unique set of columns. For each entry
    take the value of the most reliable data source included in that match.

    Parameters
    ----------
    df : pandas.Dataframe
        MultiIndex dataframe with the matched powerplants, as obtained from
        combined_dataframe() or match_multiple_datasets()
    """
    # df = get_obj_if_Acc(df)

    # define which databases are present and get their reliability_score
    sources = df.columns.levels[1]
    rel_scores = pd.Series({s: CONFIG[s]['reliability_score'] for s in sources})\
                   .sort_values(ascending=False)
    cols = CONFIG['target_columns']
    props_for_groups = {col: 'first'
                        for col in cols}
    props_for_groups.update({'YearCommisisoned': 'min',
                             'DateRetrofit': 'max',
                             'projectID': lambda x: dict(x.droplevel(0).dropna()),
                             'eic_code': 'unique'})
    props_for_groups = pd.Series(props_for_groups)[cols].to_dict()

    # set low priority on Fueltype 'Other'
    # turn it since aggregating only possible for axis=0
    sdf = df.replace({'Fueltype': {'Other': np.nan}})\
            .stack(1).reindex(rel_scores.index, level=1)\
            .groupby(level=0)\
            .agg(props_for_groups)\
            .replace({'Fueltype': {np.nan: 'Other'}})

    if show_orig_names:
        sdf = sdf.assign(**dict(df.Name))

    return sdf.pipe(clean_technology).reset_index(drop=True)