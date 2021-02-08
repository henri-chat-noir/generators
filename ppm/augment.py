from datetime import datetime
import os
import networkx as nx
import logging

import numpy as np
import pandas as pd

import globals as glob
from globals import CONFIG, SUB_CLEAN, SUB_TAG, SUB_OUT

logger = logging.getLogger(__name__)

# import _globals as glob
# from cleaning_functions import clean_powerplantname # Not sure should be requirement for further cleaning at this point
from duke import duke_cliques

def tag_cliques_for_single_ds(in_df):

    """
    Locate cliques of units which are determined to belong to the same
    powerplant.  Return the same dataframe with an additional column
    "grouped" which indicates the group that the powerplant is
    belonging to.

    Parameters
    ----------
    df : pandas.Dataframe or string
        dataframe or csv-file which should be analysed
    dataduplicates : pandas.Dataframe or string
        dataframe or name of the csv-linkfile which determines the
        link within one dataset
    """

    def mark_duplicates_in_df(df, dataduplicates):
        G = nx.DiGraph()
        G.add_nodes_from(df.index)
        G.add_edges_from((r.one, r.two) for r in dataduplicates.itertuples())
        H = G.to_undirected(reciprocal=True)

        grouped = pd.Series(np.nan, index=df.index)
        for i, inds in enumerate(nx.algorithms.clique.find_cliques(H)):
            grouped.loc[inds] = i

        return df.assign(grouped=grouped)

    df = in_df.copy()
    ds_name = df.columns.name

    # Set consistent types for certain columns and adjust cap_weighted_cols to be MW-multiplied
    cap_weighted_cols = ['Efficiency', 'Duration']
    cap_weighted_cols = [col for col in cap_weighted_cols if col in CONFIG['target_columns']]

    float_cols = []

    # string_cols = ['Name', 'Country', 'Fueltype', 'Technology', 'Set', 'File']
    # string_cols = [col for col in string_cols if col in CONFIG['target_columns']]

    df = (df
            .assign(**{col: df[col] * df.Capacity for col in cap_weighted_cols})
            .assign(lat=df.lat.astype(float), lon=df.lon.astype(float))
            # .assign(**{col: df[col].astype(str) for col in string_cols})
            )

    weighted_cols = [col for col in ['Efficiency', 'Duration']
                     if col in CONFIG['target_columns']]

    
    # if pre_clean_name: df = clean_powerplantname(df)

    print()
    logger.info(f"Tagging rows in dataset that are sufficiently similar: {ds_name}")
    
    # Country-wise effort to find matching cliques via duke
    cliques_df = None
    for country in df.Country.unique():
        country_extract = df[df.Country == country]
        dukemap_cliques = duke_cliques(country_extract, country)
        cliques_df = pd.concat([cliques_df, dukemap_cliques])
    
    df = mark_duplicates_in_df(df, cliques_df)
        
    return df

def aggregate_units_for_single_ds(in_df, pre_clean_name=False,
                    save_dukemap=True,
                    country_wise=True,
                    use_saved_dukemap=True
                    ):

    """
    Vertical cleaning of the database. Cleans the "Name"-column, sums
    up the capacity of powerplant units which are determined to belong
    to the same plant.
    """

    # Work-up / final clean of aggreated, plant-level dataframe
    
    # Set the method to which units are grouped up into plants / data selected

    def mode(x):
        return x.mode(dropna=False).at[0]

    agg_methods_dict = {'Name': mode,
         'Fueltype': mode,
         'Technology': mode,
         'Set': mode,
         'Country': mode,
         'Capacity': 'sum',
         'lat': 'mean',
         'lon': 'mean',
         'DateIn': 'min',
         'DateRetrofit': 'max',  # choose latest Retrofit-Year
         'DateMothball': 'min',
         'DateOut': 'min',
         'File': mode,
         'projectID': list,
         'EIC': set,
         'Duration': 'sum',  # note this is weighted sum
         'Volume_Mm3': 'sum',
         'DamHeight_m': 'sum',
         'StorageCapacity_MWh': 'sum',
         'Efficiency': 'mean'  # note this is weighted mean
         }

    props_for_groups = pd.Series(agg_methods_dict).reindex(CONFIG['target_columns'], axis=1).to_dict()
    df = df.groupby('grouped').agg(props_for_groups)
    df = df.replace('nan', np.nan)

    if 'EIC' in df:
        df = df.assign(EIC=df['EIC'].apply(list))

    df = (df
          .assign(**{col: df[col].div(df['Capacity'])
                     for col in weighted_cols})
          .reset_index(drop=True)
          .pipe(clean_powerplantname)
          .reindex(columns=CONFIG['target_columns'])
          )

    df.columns.name = ds_name

    return df