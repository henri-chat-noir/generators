"""
Functions for vertically cleaning a dataset.
"""

# import os
import csv
import re
import numpy as np
import pandas as pd
import networkx as nx
import logging

import _globals as glob
from _globals import CONFIG
from utils import get_name
# from diagnostics import save_debug, build_ids_list
from diagnostics import build_ids_list
import file_handling as fh

logger = logging.getLogger(__name__)


def add_geoposition_for_duke(in_df):
    """
    Returns the same pandas.Dataframe with an additional column "Geoposition"
    which concats the latitude and longitude of the powerplant in a string

    """
    if not in_df.loc[:, ['lat', 'lon']].isnull().all().all():
        out_df = (in_df
                    .assign(Geoposition=in_df[['lat', 'lon']].astype(str)
                    .apply(lambda s: ','.join(s), axis=1)
                    .replace('nan,nan', np.nan))
                    )
    else:
        out_df = in_df.assign(Geoposition=np.nan)

    return out_df

def clean_powerplantname(in_df, common_word_threshold_count=20):
    """
    Cleans the column "Name" of the database by deleting very frequent
    words, numericals and nonalphanumerical characters of the
    column. Returns a reduced dataframe with nonempty Name-column.

    Parameters
    ----------
    df : pandas.Dataframe
        dataframe to be cleaned

    """

    def drop_dup_words(dirty_string):

        word_list = dirty_string.split()
        clean_list = []
        for word in word_list:
            if word not in clean_list:
                clean_list.append(word)

        clean_string = " ".join(clean_list)
        return clean_string

    # Consider retaning all rows, even null
    out_df = in_df[in_df.OrigName.notnull()]

    # 1. Convert puntuation characters to spaces    
    punctuation = ['\.', ',', '-', '/', '\(', '\)', '\[', '\]', '"', '_', '\+', '[0-9]']
    name_series = out_df.OrigName.replace(value=' ', to_replace=punctuation, regex=True)
    name_series = name_series.astype(str)
    name_series = name_series.str.lower()
    name_series = name_series.apply(lambda x: drop_dup_words(x))

    # 2A. Company designators, general names for plants, etc. - words list that should NOT be overly relevant for matching
    stop_word_spec = glob.package_data('stop_words.txt')    
    stop_words = fh.load_text_to_list(stop_word_spec)

    # 2B. Single characters
    single_chars = ['[a-z]']

    # 2C. Ignore word replacement
    # ignore_words = unit_designators + stop_words + single_chars
    ignore_words = stop_words + single_chars
    ignore_pattern = [('(?i)(^|\s)'+x+'(?=\s|$)') for x in ignore_words]
    name_series = name_series.replace(value=' ', to_replace=ignore_pattern, regex=True)

    # 2D. Final clean-up
    unit_series = name_series.replace(['\s+', '"', 'ß'], [' ', '', 'ss'], regex=True)
    unit_series = unit_series.str.strip()

    # 3. Words occuring frequently in dataset, cw means "common words"
    cw_series = pd.Series( sum(name_series.str.split(), [])).value_counts()
    cw_list = list(cw_series[cw_series >= common_word_threshold_count].index)

    white_word_spec = glob.package_data('white_words.txt')
    white_words = fh.load_text_to_list(white_word_spec)

    for white_word in white_words:
        if white_word in cw_list:
            cw_list.remove(white_word)

    if cw_list:
        cw_pattern = [('(?i)(^|\s)'+x+'(?=\s|$)') for x in cw_list]
        name_series = name_series.replace(value=' ', to_replace=cw_pattern, regex=True)
        plant_series = name_series.str.strip()
    else:
        plant_series = unit_series.copy()

    # If cleaning completely eliminates the string, then for UnitName revert to OrigName at those locations
    null_indices = plant_series[plant_series==""].index
    unit_series[null_indices] = in_df.OrigName[null_indices]
    
    # out_df = out_df.assign(Name=name_series)
    out_df['UnitName'] = unit_series.str.title()

    # Need to work through retaining these elements for unit names, but removed from PlantName (for now)
    unit_designators = ['i', 'ii', 'iii', 'iv', 'v', 'vi', 'vii', 'viii', 'ix', 'x', 'xi', 'xii', 'xiii', 'stage']
    # unit_pattern = [('\s+'+ element + '\s+') for element in unit_designators]
    unit_pattern = [(r'\b'+ element + r'\b') for element in unit_designators]

    cw_list_ex_units = list( set(cw_list) - set(unit_designators) )
    if cw_list_ex_units:
        cw_list_ex_units.sort()
        print(f"Removed the following common words, not already saved 'stop words' with > {common_word_threshold_count} entries . . . (excludes unit designators)")
        for word in cw_list_ex_units:
            print(word)
        print()

    # Note that PlantName, used for matching, reverts back to nullstrings if so indicated earlier
    plant_series = plant_series.replace(value=' ', to_replace=unit_pattern, regex=True)
    plant_series = plant_series.replace(['\s+'], [' '], regex=True)
    plant_series = plant_series.str.strip()
    out_df['PlantName'] = plant_series.str.title()

    # Work-up something that does all caps on identified acronyms in plant names
    acronyms = ["HKW", "MVM"]

    out_df = (out_df
                # .loc[lambda x: x.Name != ""]
                .sort_values('UnitName')
                .reset_index(drop=True)
            )

    return out_df

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

def list_from_string(text_str, separators):

    pattern = '|'.join(separators)

    out_list = re.split(pattern, text_str)
    out_list = [word.strip() for word in out_list if word.strip() != ""]

    return out_list

def parse_fuel_cols(df, esource_col, raw_primary_col, raw_secondary_col=None):

    def modify_fuel_label(in_text):

        multi_whitespace = re.compile(r"\s+")
        out_text = multi_whitespace.sub(" ", in_text).strip()
        out_text = out_text.lower()

        for from_word, vals in word_mod_dict.items():
            to_word = vals['to_word']
            if out_text == from_word:
                out_text = to_word

        if in_text != out_text:
            stop = True

        return out_text

    def process_fuel_string(fuel_str):

        fuels_list = []
        separators = ['\,', ' and ', '\/', '\(', '\)', ' or ', ' & ']
        orig_fuels_list = list_from_string(fuel_str, separators)

        for fuel in orig_fuels_list:
            new_fuel = modify_fuel_label(fuel)
            fuel_label = fuel_label_lookup_dict.get(new_fuel, new_fuel)
            if fuel_label != "":
                fuels_list.append(fuel_label)

        return fuels_list

    def build_lookup_dict(fuel_tree_df):

        lookup_dict = {}
        for idx, row_data in fuel_tree_df.iterrows():
            syns = row_data['syns']
            if pd.notnull(syns):
                syns = eval(syns)
            else:
                syns = set()

            abbs = row_data['abbs']
            if pd.notnull(abbs):
                abbs = eval(abbs)
            else:
                abbs = set()

            test_words = syns.union(abbs) # Add together synonyms and abbreviations
            for fuel_word in test_words:
                # lookup_dict[fuel_word] = row_data['fuel_label']
                lookup_dict[fuel_word] = idx

        return lookup_dict

    def find_fuel_hierarchy(fuel, tree_df):

        try:
            row_data = tree_df.loc[fuel]
            if row_data.fuel_level == 1: # Top-level esource designation only
                fuel_species = ""
                fuel_genus = ""

            elif row_data.fuel_level == 2: # Genus-level indicator
                fuel_species = ""
                fuel_genus = fuel

            elif row_data.fuel_level == 3: # Species-level designation, so must have fuel_genus value in table
                fuel_species = fuel
                fuel_genus = row_data.fuel_genus

            esource_id = row_data['esource_id']
            
        except:
            print(fuel)
            fuel_species = ""
            fuel_genus = ""
            esource_id = ""
            stop = True
            
        return fuel_species, fuel_genus, esource_id

    word_mod_fn = "fuel_mods.csv"
    word_mod_spec = glob.package_data(word_mod_fn)
    word_mod_dict = fh.load_csv_to_dict(word_mod_spec)

    fuel_tree_fn = "fuel_tree.csv"
    fuel_tree_spec = glob.package_data(fuel_tree_fn)
    fuel_tree_df = pd.read_csv(fuel_tree_spec, index_col='fuel_label')

    fuel_label_lookup_dict = build_lookup_dict(fuel_tree_df)

    col_sep = "^"
    primary_col_list = []
    secondary_col_list = []
    for idx, row_data in df.iterrows():
        
        fuels_list = []
        if pd.notnull(row_data[raw_primary_col]):
            primary_str = row_data[raw_primary_col]
            fuels_list += process_fuel_string(primary_str)
            
        if raw_secondary_col is not None and pd.notnull(row_data[raw_secondary_col]):
            secondary_str = row_data[raw_secondary_col]
            fuels_list += process_fuel_string(secondary_str)

        esource = row_data[esource_col]
        if not fuels_list and esource != "": # Only load 'type' column into fuels list if nothing else to go on.
            fuels_list.append(esource.lower()) 

        if not fuels_list:
            print("Nothing to go on")
            stop = True

        primary_label = fuels_list[0] # This will assign either 'Type' or first entry in primary_fuel col as primary_label
        if len(fuels_list) > 1:
            secondary_fuels = fuels_list[1:]
        else:
            secondary_fuels = []

        fuel_species, fuel_genus, esource_id = find_fuel_hierarchy(primary_label, fuel_tree_df)
        df.loc[idx, 'PrimaryFuel'] = primary_label
        df.loc[idx, 'FuelSpecies'] = fuel_species
        df.loc[idx, 'FuelGenus'] = fuel_genus
        df.loc[idx, 'esource_id'] = esource_id
    
        if len(secondary_fuels) > 0:
            secondary_fuels = set(secondary_fuels)
        else:
            secondary_fuels = ""
        df.loc[idx, 'SecondaryFuels'] = str(secondary_fuels)

    useless_indicators = {'mixed fuel', 'other'}
    for useless_word in useless_indicators:
        # primary_col_list.remove(useless_word)
        pass

    return df

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

def gather_set_info(df, search_col=['OrigName', 'Fueltype', 'Technology']):
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
        val_ser = df[i].dropna()
        if i == 'OrigName':
            val_ser = val_ser.astype(str)
        isCHP_b = val_ser.str.contains(pattern, case=False).reindex(df.index).fillna(False)
        Set.loc[isCHP_b] = 'CHP'

    pattern = '|'.join(['battery', 'storage'])
    for i in search_col:
        isStore_b = df[i].dropna().astype(str).str.contains(pattern, case=False) \
            .reindex(df.index).fillna(False)
        Set.loc[isStore_b] = 'Store'

    df = df.assign(Set=Set)
    df.loc[:, 'Set'].fillna('PP', inplace=True)
    return df

def gather_technology_info(df, search_col=['OrigName', 'Fueltype']):
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
