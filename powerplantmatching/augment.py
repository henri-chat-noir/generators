import os
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

from core import _data_out
from utils import set_column_name
from cleaning_functions import clean_powerplantname, cliques
from duke import duke

def aggregate_units(df, dataset_name=None,
                    pre_clean_name=True,
                    save_aggregation=True,
                    country_wise=True,
                    use_saved_aggregation=False,
                    config=None):

    """
    Vertical cleaning of the database. Cleans the "Name"-column, sums
    up the capacity of powerplant units which are determined to belong
    to the same plant.

    Parameters
    ----------
    df : pandas.Dataframe or string
        Dataframe or name to use for the resulting database
    dataset_name : str, default None
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
    
    try:
        ds_name = df.name
    except:
        no_name = True
        ds_name = None

    weighted_cols = [col for col in ['Efficiency', 'Duration']
                     if col in config['target_columns']]

    columns = df.columns
    # print(columns)
    stop = True

    df = (df.assign(**{col: df[col] * df.Capacity for col in weighted_cols})
            .assign(lat=df.lat.astype(float),
                    lon=df.lon.astype(float))
            .assign(**{col: df[col].astype(str) for col in
                       ['Name', 'Country', 'Fueltype',
                        'Technology', 'Set', 'File']
                       if col in config['target_columns']}))

    def mode(x):
        return x.mode(dropna=False).at[0]

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
         }).reindex(config['target_columns'], axis=1).to_dict()

    dataset_name = get_name(df) if dataset_name is None else dataset_name

    if pre_clean_name:
        df = clean_powerplantname(df)

    print()
    logger.info("Aggregating blocks to entire units in '{}'."
                .format(dataset_name))

    path_name = _data_out(f'aggregations/aggregation_groups_{dataset_name}.csv')

    if use_saved_aggregation & save_aggregation:
        if os.path.exists(path_name):
            logger.info("Reading saved aggregation groups for dataset '{}'."
                        .format(dataset_name))
            groups = (pd.read_csv(path_name, header=None, index_col=0)
                        .reindex(index=df.index))
            df = df.assign(grouped=groups.values)
        else:
            if 'grouped' in df:
                df.drop('grouped', axis=1, inplace=True)
    else:
        logger.info(f"Not using saved aggregation groups for dataset "
                    f"'{dataset_name}'.")

    if 'grouped' not in df:
        if country_wise:
            duplicates = pd.concat([duke(df.query('Country == @c'))
                                    for c in df.Country.unique()])
        else:
            duplicates = duke(df)
        df = cliques(df, duplicates)
        if save_aggregation:
            df.grouped.to_csv(path_name, header=False)

    df = df.groupby('grouped').agg(props_for_groups)
    df = df.replace('nan', np.nan)

    if 'EIC' in df:
        df = df.assign(EIC=df['EIC'].apply(list))

    df = (df
          .assign(**{col: df[col].div(df['Capacity'])
                     for col in weighted_cols})
          .reset_index(drop=True)
          .pipe(clean_powerplantname)
          .reindex(columns=config['target_columns'])
          .pipe(set_column_name, dataset_name))

    return df
