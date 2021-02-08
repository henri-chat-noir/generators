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

import globals as glob
from globals import CONFIG
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

def clean_name(in_df, common_word_threshold_count=10):
    """
   
    """

    def substitute_regex_patterns(in_names):

        out_names = in_names.copy()

        internal_ac_dots = r"(?<=\b\w)\.(?=\w\b)"
        # ac_dots = r"\.(?!(\S[^. ])|\d)"

        ac_dots = r"\.(?!(\S[^. ])|\d){2}"

        out_names = out_names.replace(value="", to_replace=internal_ac_dots, regex=True)

        start_end_apostrophes = [r"\B'\b|\b'\B"]
        out_names = out_names.replace(value=' ', to_replace=start_end_apostrophes, regex=True)

        # Now all remaining apostrophes are intra-word, but no way to prevent regex from seeing as word boundary in itself
        # Therefore temporarily replace with code-word (a word on it's own) that can be swapped back in at end of processing
        out_names = out_names.replace(value=quote_code, to_replace=r"'", regex=True)

        san_pattern = [r'\bS\.']
        out_names = out_names.replace(value='San ', to_replace=san_pattern, regex=True)

        out_names = out_names.replace(value='Villa ', to_replace=[r'V\.'], regex=True)
        out_names = out_names.replace(value='Centrale', to_replace=[r'\bC\.LE\b'], regex=True)

        saint_pattern = [r'\bS[Tt]\.']
        out_names = out_names.replace(value='Saint ', to_replace=saint_pattern, regex=True)
        # san_rep_pattern = [('(?i)(^|\s)' + word + '(?=\s|$)') for word in keyword_drop_words]

        return out_names

    def drop_dup_words(dirty_string):

        word_list = dirty_string.split()
        clean_list = []
        for word in word_list:
            if word not in clean_list:
                clean_list.append(word)

        clean_string = " ".join(clean_list)
        return clean_string

    def parse_unit_tags(in_names):

        unit_words = ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'XI', 'XII', 'XIII']
        # unit_words = ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'XI', 'XII', 'XIII']
        unit_pattern_list = [r'\b' + word + r'\b' for word in unit_words]

        unit_pattern_list.append(r'\b[A-G]\b')

        # Digits can/should be extracted, even if don't have word boundary at start
        # Note this is deliberately constucted to exract suffix greedingly to end of string (from digit onwards)
        # This string is intended to (first pull) digit-including 'words' out of string
        # So long as they aren't part of word that is itself at start of string
        unit_pattern_list.append(r'(?!^)(?=\b)[a-zA-Z]{0,3}[0-9]+.*')
        
        # Then simple extraction of residual digit-onwards elements from single-word entries like HERON3-4A --> 3-4A
        unit_pattern_list.append(r'\d+.*')

        unit_pattern_string = "|".join(unit_pattern_list)
        extract_pattern_group = r'(' + unit_pattern_string + r')'
        unit_tags = in_names.str.extract(extract_pattern_group, expand=False)
        
        out_names = in_names.replace(value=' ', to_replace=unit_pattern_list, regex=True)

        return out_names, unit_tags

    def clean_dross(in_names):
        """
        Cast to string
        Lowercase
        Convert puntuation characters to spaces
        Drop duplicated words
        
        """
        # clean_names = dirty_names.astype(str)
        out_names = in_names.copy()
        out_names = out_names.str.lower()

        # punctuation = ["'", '\.', ',', '-', '/', '\(', '\)', '\[', '\]', '"', '_', '\+', '[0-9]']
        punctuation = [r'\.', r" '", r',', r'-', r'/', r'\(', r'\)', r'\[', r'\]', r'"', r'_', r'\+', r'\\'] # exclude dots
        out_names = out_names.replace(value=' ', to_replace=punctuation, regex=True)
    
        # clean_names = clean_names.replace(value="", to_replace="\.", regex=True)
        out_names = out_names.replace(['\s+', 'ß'], [' ', 'ss'], regex=True) # multi-space, German plural to double-s

        single_char_pattern = [('(?i)(^|\s)' + '[a-z]' + '(?=\s|$)')]
        # clean_names = clean_names.replace(value=' ', to_replace=single_char_pattern, regex=True)
        # out_names = out_names.apply( lambda x: drop_dup_words(x) )

        out_names = out_names.replace(['\s+'], [' ',], regex=True) # multi-space
        out_names = out_names.str.strip()

        return out_names

    # By removing space at front it 'jams' code against any character that might precede it
    # Therefore D'Angelo, say, would be split into Dqqqqq and Angelo, and the prefix is not going 
    # To get captured as single-letter unit designator "D"
    quote_code = r'qqqqq '
    
    # Consider retaning all rows, even null
    # out_df = in_df[in_df.OrigName.notnull()]

    out_df = in_df.copy()
    orig_names = out_df['OrigName']

    # Work-up something that does all caps on identified acronyms in plant names
    acronyms = ["HKW", "MVM"]


    # REGEX REPLACEMENT WORK - PRIOR TO ANY UNIT EXTRACTION LOWER-CASING, ETC.
    # ====================================================

    plant_names = substitute_regex_patterns(orig_names)


    # UNIT TAG EXTRACTION - FIRST STEP BEFORE DROSS-CLEANING, ETC.
    # ===========================================================
    # Useful to initally 'collapse' acronyms, e.g. S.P.A. to 'words', rather than separate letters
    # no_dots = orig_names.replace(value="", to_replace=r'\.', regex=True)
    
    # Extract unit info before switch to lower case, to retain original character of text for unit_tags
    (plant_names, unit_tags) = parse_unit_tags(plant_names) 
    out_df['UnitTag'] = unit_tags


    # PLANT NAME FORMATION
    # =====================
    clean_plant_names_pre_stop = clean_dross(plant_names) # Starting point for both PlantName and KeywordName
 
    # stop_words = company designators, common sub-words, general names for plants, etc.
    # words list that is either or both a) adds clutter, and/or b) NOT be overly relevant for matching
    stop_word_spec = glob.ref_data('stop_words.csv')    
    stop_words_df = pd.read_csv(stop_word_spec, encoding='UTF-8')

    # white_word_spec = glob.ref_data('white_words.txt')
    # white_words = fh.load_text_to_list(white_word_spec)
    # white_words = set(white_words)

    # Amongs stop words, GEO, SUB, PREP, and SITE type words are RETAINED in PlantName (but not keywords)
    stop_word_types_spec = glob.package_data('stop_word_types.csv')
    stop_word_types_df = pd.read_csv(stop_word_types_spec, index_col='word_type')
    name_drop_types = frozenset( stop_word_types_df[stop_word_types_df.name_retain != 1].index )
    gross_name_drop_words = set( stop_words_df[ stop_words_df.word_type.isin(name_drop_types) ].word_string )

    name_white_words = set( stop_words_df[stop_words_df.retain_in_name == 1].word_string )
    name_drop_words = gross_name_drop_words - name_white_words

    name_drop_pattern = [('(?i)(^|\s)'+ word +'(?=\s|$)') for word in name_drop_words]
    clean_plant_names = clean_plant_names_pre_stop.replace(value=' ', to_replace=name_drop_pattern, regex=True)
    
    clean_plant_names = clean_plant_names.replace(value=r"'", to_replace=quote_code, regex=True)

    clean_names = clean_plant_names.replace(['\s+'], [' ',], regex=True) # multi-space
    clean_plant_names = clean_plant_names.str.strip()
    
    # If cleaning and stop word replacement completely eliminates the string
    # then revert PlantName to position prior to stop word removal (at those locations)
    null_indices = clean_plant_names[clean_plant_names==""].index
    clean_plant_names[null_indices] = clean_plant_names_pre_stop[null_indices]
    
    # Use this variant, with designated esource-related 'suffix', as initial "places" Google map search
    out_df['PlantName'] = clean_plant_names.str.title() 


    # BUILD KEYWORD NAME
    # ==================
    gross_keyword_drop_words = set( stop_words_df['word_string'] )
    keyword_white_words = set( stop_words_df[stop_words_df.retain_in_keyword == 1].word_string )
    keyword_drop_words = gross_keyword_drop_words - keyword_white_words

    keyword_drop_pattern = [('(?i)(^|\s)' + word + '(?=\s|$)') for word in keyword_drop_words]
    keyword_names = clean_plant_names_pre_stop.replace(value=' ', to_replace=keyword_drop_pattern, regex=True)
    
    keyword_names = keyword_names.replace(value=r"'", to_replace=quote_code, regex=True)
    keyword_names = keyword_names.replace(['\s+'], [' ',], regex=True) # multi-space
    keyword_names = keyword_names.str.strip()
    out_df['KeywordName'] = keyword_names.str.title()

    
    # COMMON WORD EVALUATION
    # =================================================================================
    # After defined stop word removal, evaluation of frequently occurring words
    # for potential stop_word dictionary development (so excludes words already white-listed)
    cw_series = pd.Series( sum(keyword_names.str.split(), [])).value_counts()
    cw_set = set( cw_series[cw_series >= common_word_threshold_count].index )
    report_cw_set = cw_set - keyword_white_words

    if report_cw_set:
        cw_list = list(report_cw_set)
        cw_list.sort()
        print(f"Discovered the following additional common words in KeywordName with > {common_word_threshold_count} occurences . . .")
        for word in cw_list:
            print(word)
        print()

    out_df = (out_df
                # .loc[lambda x: x.Name != ""]
                .sort_values('PlantName')
                .reset_index(drop=True)
            )

    rows = len(out_df.index)
    stop = True

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
