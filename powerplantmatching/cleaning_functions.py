"""
Functions for vertically cleaning a dataset.
"""

# import os
import numpy as np
import pandas as pd
import networkx as nx
import logging

from _globals import CONFIG
from utils import get_name, set_column_name

logger = logging.getLogger(__name__)

def clean_powerplantname(df):
    """
    Cleans the column "Name" of the database by deleting very frequent
    words, numericals and nonalphanumerical characters of the
    column. Returns a reduced dataframe with nonempty Name-column.

    Parameters
    ----------
    df : pandas.Dataframe
        dataframe to be cleaned

    """
    # df = get_obj_if_Acc(df)
    df = df[df.Name.notnull()]
    name = df.Name.replace(regex=True, value=' ',
                           to_replace=['-', '/', ',', '\(', '\)', '\[', '\]',
                                       '"', '_', '\+', '[0-9]'])

    common_words = pd.Series(sum(name.str.split(), [])).value_counts()
    cw = list(common_words[common_words >= 20].index)

    pattern = [('(?i)(^|\s)'+x+'(?=\s|$)')
               for x in (cw +
                         ['[a-z]', 'I', 'II', 'III', 'IV', 'V', 'VI', 'VII',
                          'VIII', 'IX', 'X', 'XI', 'Grupo', 'parque', 'eolico',
                          'gas', 'biomasa', 'COGENERACION', 'gt', 'unnamed',
                          'tratamiento de purines', 'planta', 'de', 'la',
                          'station', 'power', 'storage', 'plant', 'stage',
                          'pumped', 'project', 'dt', 'gud', 'hkw', 'kbr',
                          'Kernkraft', 'Kernkraftwerk', 'kwg', 'krb', 'ohu',
                          'gkn', 'Gemeinschaftskernkraftwerk', 'kki', 'kkp',
                          'kle', 'wkw', 'rwe', 'bis', 'nordsee', 'ostsee',
                          'dampfturbinenanlage', 'ikw', 'kw', 'kohlekraftwerk',
                          'raffineriekraftwerk', 'Kraftwerke', 'Psw'])]
    name = (name
            .replace(regex=True, to_replace=pattern, value=' ')
            .replace(['\s+', '"', 'ß'], [' ', '', 'ss'], regex=True)
            .str.strip()
            .str.capitalize())

    return (df
            .assign(Name=name)
            .loc[lambda x: x.Name != '']
            .sort_values('Name')
            .reset_index(drop=True))

def clean_technology(df, generalize_hydros=False):
    """
    Clean the 'Technology' by condensing down the value into one claim. This
    procedure might reduce the scope of information, however is crucial for
    comparing different data sources.

    Parameter
    ---------
    search_col : list, default is ['Name', 'Fueltype', 'Technology']
        Specify the columns to be parsed
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()

    """
    tech = df['Technology'].dropna()
    if len(tech) == 0:
        return df
    tech = tech.replace(
        {' and ': ', ', ' Power Plant': '', 'Battery': ''}, regex=True)
    if generalize_hydros:
        tech[tech.str.contains('pump', case=False)] = 'Pumped Storage'
        tech[tech.str.contains('reservoir|lake', case=False)] = 'Reservoir'
        tech[tech.str.contains('run-of-river|weir|water', case=False)] =\
            'Run-Of-River'
        tech[tech.str.contains('dam', case=False)] = 'Reservoir'
    tech = tech.replace({'Gas turbine': 'OCGT'})
    tech[tech.str.contains('combined cycle|combustion', case=False)] = 'CCGT'
    tech[tech.str.contains('steam turbine|critical thermal', case=False)] =\
        'Steam Turbine'
    tech[tech.str.contains('ocgt|open cycle', case=False)] = 'OCGT'
    tech = (tech.str.title()
                .str.split(', ')
                .apply(lambda x: ', '.join(i.strip() for i in np.unique(x))))
    tech = tech.replace({'Ccgt': 'CCGT', 'Ocgt': 'OCGT'}, regex=True)
    return df.assign(Technology=tech)

def gather_fueltype_info(df, search_col=['Name', 'Technology']):
    """
    Parses in search_col columns for distinct coal specifications, e.g.
    'lignite', and passes this information to the 'Fueltype' column.

    Parameter
    ---------
    search_col : list, default is ['Name', 'Technology']
        Specify the columns to be parsed
    """
    fueltype = pd.Series(df['Fueltype'])

    for i in search_col:
        found_b = df[i].dropna().str.contains('(?i)lignite|brown')
        fueltype.loc[found_b.reindex(fueltype.index,
                                     fill_value=False)] = 'Lignite'
    fueltype.replace({'Coal': 'Hard Coal'}, inplace=True)

    return df.assign(Fueltype=fueltype)

def gather_set_info(df, search_col=['Name', 'Fueltype', 'Technology']):
    """
    Parses in search_col columns for distinct set specifications, e.g.
    'Store', and passes this information to the 'Set' column.

    Parameter
    ---------
    search_col : list, default is ['Name', 'Fueltype', 'Technology']
        Specify the columns to be parsed
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()

    """
    Set = (df['Set'].copy()
           if 'Set' in df
           else pd.Series(index=df.index))

    pattern = '|'.join(['heizkraftwerk', 'hkw', 'chp', 'bhkw', 'cogeneration',
                        'power and heat', 'heat and power'])
    for i in search_col:
        isCHP_b = df[i].dropna().str.contains(pattern, case=False)\
            .reindex(df.index).fillna(False)
        Set.loc[isCHP_b] = 'CHP'

    pattern = '|'.join(['battery', 'storage'])
    for i in search_col:
        isStore_b = df[i].dropna().str.contains(pattern, case=False) \
            .reindex(df.index).fillna(False)
        Set.loc[isStore_b] = 'Store'

    df = df.assign(Set=Set)
    df.loc[:, 'Set'].fillna('PP', inplace=True)
    return df

def gather_technology_info(df, search_col=['Name', 'Fueltype']):
    """
    Parses in search_col columns for distinct technology specifications, e.g.
    'Run-of-River', and passes this information to the 'Technology' column.

    Parameter
    ---------
    search_col : list, default is ['Name', 'Fueltype']
        Specify the columns to be parsed
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()

    """
    
    technology = (df['Technology'].dropna()
                  if 'Technology' in df
                  else pd.Series())

    pattern = '|'.join(('(?i)'+x) for x in CONFIG['target_technologies'])
    for i in search_col:
        found = (df[i].dropna()
                 .str.findall(pattern)
                 .loc[lambda s: s.str.len() > 0]
                 .str.join(sep=', '))

        exists_i = technology.index.intersection(found.index)
        if len(exists_i) > 0:
            technology.loc[exists_i] = (technology.loc[exists_i].str
                                        .cat(found.loc[exists_i], sep=', '))

        new_i = found.index.difference(technology.index)
        technology = technology.append(found[new_i])

    return df.assign(Technology=technology)
