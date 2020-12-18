import os
import networkx as nx
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

from _globals import _set_path, CONFIG, SUB_TAG, SUB_OUT
from utils import set_column_name
from cleaning_functions import clean_powerplantname # Not sure should be requirement for further cleaning at this point
from duke import duke_cliques

def tag_cliques(df, dataduplicates):

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

    G = nx.DiGraph()
    G.add_nodes_from(df.index)
    G.add_edges_from((r.one, r.two) for r in dataduplicates.itertuples())
    H = G.to_undirected(reciprocal=True)

    grouped = pd.Series(np.nan, index=df.index)
    for i, inds in enumerate(nx.algorithms.clique.find_cliques(H)):
        grouped.loc[inds] = i

    return df.assign(grouped=grouped)

def aggregate_units(in_df, ds_name=None,
                    pre_clean_name=True,
                    save_dukemap=True,
                    country_wise=True,
                    use_saved_dukemap=True
                    ):

    """
    Vertical cleaning of the database. Cleans the "Name"-column, sums
    up the capacity of powerplant units which are determined to belong
    to the same plant.

    Parameters
    ----------
    df : pandas.Dataframe or string
        Dataframe or name to use for the resulting database
    ds_name : str, default None
        Specify the name of your df, required if use_saved_aggregation is set
        to True.
    pre_clean_name : Boolean, default True
        Whether to clean the 'Name'-column before aggregating.
    use_saved_aggregation : bool (default False):
        Whether to use the automaticly saved aggregation file, which
        is stored in data/out/default/aggregations/aggregation_groups_XX.csv
        with XX being the name for the dataset. This saves time if you
        want to have aggregated powerplants without running the
        aggregation algorithm again
    """
    
    df = in_df.copy()

    weighted_cols = [col for col in ['Efficiency', 'Duration']
                     if col in CONFIG['target_columns']]

    df = (df.assign(**{col: df[col] * df.Capacity for col in weighted_cols})
            .assign(lat=df.lat.astype(float),
                    lon=df.lon.astype(float))
            .assign(**{col: df[col].astype(str) for col in
                       ['Name', 'Country', 'Fueltype',
                        'Technology', 'Set', 'File']
                       if col in CONFIG['target_columns']}))

    def mode(x):
        return x.mode(dropna=False).at[0]

    # Set the method to which units are grouped up into plants / data selected
    props_for_groups = pd.Series(
        {'Name': mode,
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
         }).reindex(CONFIG['target_columns'], axis=1).to_dict()

    if ds_name is None: ds_name = get_name(df) 
    if pre_clean_name: df = clean_powerplantname(df)

    print()
    logger.info(f"Aggregating blocks to entire units in {ds_name}")
      
    dukemap_spec = _set_path(f"{ds_name}\\{ds_name}_dukemap.csv", SUB_TAG)

    if use_saved_dukemap and save_dukemap:
        if os.path.exists(dukemap_spec):
            logger.info(f"Reading saved Duke map for dataset {ds_name}")
            dukemap = pd.read_csv(dukemap_spec, header=None, index_col=0).reindex(index=df.index)
            df = df.assign(grouped=dukemap.values)
        else:
            if 'grouped' in df:
                df.drop('grouped', axis=1, inplace=True)
    else:
        logger.info(f"Not using saved aggregation groups for dataset {ds_name}")

    # Find matching cliques via duke
    if 'grouped' not in df:
        if country_wise:
            # duplicates = pd.concat( [duke(df.query('Country == @c')) for c in df.Country.unique()])
            duplicates = None
            for country in df.Country.unique():
                df_extract = df.query('Country == @country')
                df_extract = duke_cliques(df_extract, country)
                duplicates = pd.concat([duplicates, df_extract])
        else:
            duplicates = duke_cliques(df)
        
        df = tag_cliques(df, duplicates)

        if save_dukemap:
            df.grouped.to_csv(dukemap_spec, header=False)

    tag_spec = _set_path(f"{ds_name}_tagged.csv", SUB_TAG)
    df.to_csv(tag_spec, header=True)          


    # Work-up / final clean of aggreated, plant-level dataframe
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
          .pipe(set_column_name, ds_name))

    return df