"""
Collection of power plant data bases and statistical data
"""

import numpy as np
import pandas as pd
import requests
import xml.etree.ElementTree as ET
import re
import pycountry
import logging

import entsoe as entsoe_api
from globals import CONFIG, SUB_LAND
import globals as glob
import diagnostics as diag 

from utils import (fill_geoposition, correct_manually,
                    config_filter, convert_alpha2_to_country, convert_to_num)

from heuristics import scale_to_net_capacities

from cleaning_functions import (gather_fueltype_info, gather_set_info,
                       gather_technology_info, clean_name,
                       clean_technology)

logger = logging.getLogger(__name__)
cget = pycountry.countries.get
net_caps = CONFIG['display_net_caps']

def CARMA(in_df, run__diag=False):
    """
    """
    
    out_df = in_df.copy()

    # 1. Rename columns
    col_rename_dict = {'Geoposition': 'Geoposition',
                             'cap': 'Capacity',
                             'city': 'location',
                             'country': 'Country',
                             'fuel1': 'Fueltype',
                             'lat': 'lat',
                             'lon': 'lon',
                             'plant': 'OrigName',
                             'plant.id': 'projectID'}
    out_df.rename(columns=col_rename_dict, inplace=True)

    # 2. Revise project_ID column by pre-pending "CARMA" to existing value
    out_df = out_df.assign(projectID=lambda out_df: 'CARMA' + out_df.projectID.astype(str))
    
    
    # 3. Select only those rows where Country is in list of target countries
    out_df = out_df.loc[lambda out_df: out_df.Country.isin(CONFIG['target_countries'])]

    # 4. Rename Fueltype values
    fuel_rename_dict = {'COAL': 'Hard Coal',
                                    'WAT': 'Hydro',
                                    'FGAS': 'Natural Gas',
                                    'NUC': 'Nuclear',
                                    'FLIQ': 'Oil',
                                    'WIND': 'Wind',
                                    'EMIT': 'Other',
                                    'GEO': 'Geothermal',
                                    'WSTH': 'Waste',
                                    'SUN': 'Solar',
                                    'BLIQ': 'Bioenergy',
                                    'BGAS': 'Bioenergy',
                                    'BSOL': 'Bioenergy',
                                    'OTH': 'Other'}
    out_df['Fueltype'].replace(fuel_rename_dict, inplace=True)

    # out_df = out_df.drop_duplicates() - evaluate why this is required

    # 5. Clean powerplant name, and other piped functions
    out_df = (out_df
                .pipe(clean_name)
                .pipe(config_filter, name='CARMA')
                .pipe(gather_technology_info)
                .pipe(gather_set_info)
                .pipe(clean_technology)
                .pipe(scale_to_net_capacities, not CONFIG['CARMA']['net_capacity'])
                .pipe(correct_manually, 'CARMA')
                )
    out_df = out_df.set_index('projectID', verify_integrity=True)

    return out_df # CARMA

def ENTSOE(in_df):
    
    # Copy just to ensure can compare with original for debugging
    # Also allows retention of dataframe meta data that seems to be reset on each copy
    out_df = in_df.copy()
    rows = len(out_df.index)

    # 1. Column renaming to ensure existing data, that needs to,
    #    aligns with names in standard 'target columns' (see next)
    out_df = out_df.rename(columns={'psrType': 'psr_code',
                               'quantity': 'Capacity',
                               'registeredResource.mRID': 'projectID',
                               'registeredResource.name': 'OrigName'})

    rows = len(out_df.index)

    # 2.  Work from psr_code column and map to esource_id and Fueltype label
    psr_map_spec = glob.package_data('psrcode_esource_map.csv')
    psr_map_df = pd.read_csv(psr_map_spec)
    psr_map_dict = dict( zip(psr_map_df['psr_code'], psr_map_df['esource_id']) )
    out_df['esource_id'] = out_df['psr_code'].map(psr_map_dict)

    fuel_map = entsoe_api.mappings.PSRTYPE_MAPPINGS
    out_df['Fueltype'] = out_df['psr_code'].map(fuel_map)

    # 3.  Re-set to essentially add any/all of target columns not in original dataset
    target_cols = CONFIG['target_columns']
    out_df = out_df.reindex(columns=target_cols)
    rows = len(out_df.index)
    
    # 4.  Look to any fundamental rowID and ensure dataset de-duplicated on/for it
    # Note this is a problem as/when we deal with versions / datasets over time,
    # So need to build a surrogate key
    out_df = out_df.drop_duplicates('projectID') # Duplicated EIC codes drop dataframe to 2,203 rows
    rows = len(out_df.index)

    # 5.  Value assignments to five (5) standard colunms: EIC, Country, Name, and Fueltype, Capacity
    #     For Capacity, type conversion to numeric
    #     Question on what to do with Capacity <= 0 -- currently dropped via .query > 0

    # First 2 characters of projectID (originally revisteredResource) are 2-digit number linked to country
    # This then is mapped to alpha_2 country codes in entsoe-specifc csv
    # God only knows where this was derived from -- manually by code developers / inspection?
    country_map_entsoe = pd.read_csv(glob.ref_data('entsoe_country_codes.csv'), index_col=0)
    country_map_entsoe = country_map_entsoe.rename(index=str).Country

    fueltype_rename_entsoe = {'Fossil Hard coal': 'Hard Coal',
                         'Fossil Coal-derived gas': 'Other',
                         '.*Hydro.*': 'Hydro',
                         '.*Oil.*': 'Oil',
                         '.*Peat': 'Bioenergy',
                         'Fossil Brown coal/Lignite': 'Lignite',
                         'Biomass': 'Bioenergy',
                         'Fossil Gas': 'Natural Gas',
                         'Marine': 'Other',
                         'Wind Offshore': 'Offshore',
                         'Wind Onshore': 'Onshore'}

    out_df.EIC = out_df.projectID
    out_df.Country = out_df.projectID.str[:2].map(country_map_entsoe)
    # out_df.Name = out_df.Name.str.title()
    out_df.Fueltype = out_df.Fueltype.replace(fueltype_rename_entsoe, regex=True)
    out_df.Capacity = pd.to_numeric(out_df.Capacity)
    rows = len(out_df.index) # still 2203 here

    # 6. Row filtering, e.g. dropping / assessing Capacity == 0
    cap_zero_rows = out_df[out_df.Capacity == 0]
    cap_neg_rows = out_df[out_df.Capacity < 0]
    out_df = out_df.query('Capacity > 0') # drops to 2186 here
    rows = len(out_df.index)

    # 7. Cleaning function pipes
    out_df = (out_df
                .pipe(convert_alpha2_to_country)
                .pipe(clean_name) # retains 2186 rows through name cleaning
                .pipe(fill_geoposition, use_saved_locations=True, saved_only=True)
                .pipe(gather_technology_info)
                .pipe(gather_set_info)
                .pipe(clean_technology)
                .pipe(config_filter, name='ENTSOE')
                .pipe(correct_manually, 'ENTSOE')
            )

    out_df = out_df.set_index('projectID') # 1755 rows at this point
    rows = len(out_df.index)

    return out_df # ENTSOE

def ESE(in_df):
    """
    New URL: https://www.sandia.gov/ess-ssl/global-energy-storage-database/
    Download URL: https://www.sandia.gov/ess-ssl/download/4440/
    Filename: GESDB_Projects_11_17_2020.xlsx

    """
    target_countries = CONFIG['target_countries']
    out_df = in_df.copy()

    out_df = out_df.rename(columns=str.strip)

    out_df = out_df.rename(columns={'Title': 'OrigName',
                             'Technology Mid-Type': 'Technology',
                             'Longitude': 'lon',
                             'Latitude': 'lat',
                             'Technology Broad Category': 'Fueltype'})

    out_df = out_df.assign(Set='Store',
                    projectID = 'ESE' + out_df.index.astype(str),
                    DateIn = lambda out_df: (out_df['Commissioned'].str[-4:].apply(pd.to_numeric, errors='coerce')),
                    Capacity= out_df['Rated Power (kW)'].apply(convert_to_num) / 1e3)

    out_df = out_df.query("Status == 'Operational' & Country in @target_countries")

    out_df = (out_df
                .pipe(clean_name)
                .pipe(clean_technology, generalize_hydros=True)
                .replace(dict(Fueltype={u'Electro-chemical': 'Battery', u'Pumped Hydro Storage': 'Hydro'}))
                .pipe(config_filter, name='ESE')
                # .pipe(correct_manually, 'ESE')
            )

    out_df = out_df.set_index('projectID', verify_integrity=True)
    return out_df # ESE

def GEO(in_df):
    """
    Importer for the GEO database.

    """

    countries = CONFIG['target_countries']
    out_df = in_df.copy()

    # 1. Column rename
    col_rename_dict = {'GEO_Assigned_Identification_Number': 'projectID',
                   'Name': 'OrigName',
                   'Type': 'Fueltype',
                   'Type_of_Plant_rng1': 'Technology',
                   'Type_of_Fuel_rng1_Primary': 'FuelClassification1',
                   'Type_of_Fuel_rng2_Secondary': 'FuelClassification2',
                   'Country': 'Country',
                   'Design_Capacity_MWe_nbr': 'Capacity',
                   'Year_Project_Commissioned': 'DateIn',
                   'Year_rng1_yr1': 'DateRetrofit',
                   'Longitude_Start': 'lon',
                   'Latitude_Start': 'lat'}

    out_df = out_df.rename(columns=col_rename_dict)

    # 2. Load and preparation of 'units' table
    # units_df = parse_if_not_stored('GEO_units', low_memory=False)
    path = glob.set_path('global_energy_observatory_ppl_units.csv', SUB_LAND)
    units_df = pd.read_csv(path, low_memory=False)

    # First 4 characters of string are year -- unclear couldn't be int type
    units_df['DateIn'] = units_df.Date_Commissioned_dt.str[:4].astype(float)

    # Convert efficiency (text percent) to real number
    units_df['Effiency'] = units_df.Unit_Efficiency_Percent.str.replace('%', '').astype(float) / 100

    # Group by equivalent of projectID, and apply min/max list for DateIn and mean to efficiency
    grouped_units_df = units_df.groupby('GEO_Assigned_Identification_Number').agg({'DateIn': [min, max], 'Effiency': 'mean'})

    min_DateIn_year = out_df.projectID.map(grouped_units_df.DateIn['min'])
    out_df['DateIn'] = out_df.DateIn.str[:4].apply(pd.to_numeric, errors='coerce').where(lambda x: x > 1900).fillna(min_DateIn_year)

    max_DateIn_year = out_df.projectID.map(grouped_units_df.DateIn['max'])
    out_df['DateRetrofit'] = out_df.DateRetrofit.astype(float).fillna(max_DateIn_year)

    efficiency = grouped_units_df.Effiency['mean']
    out_df['Effiency'] = out_df.projectID.map(efficiency)

    out_df = out_df.assign(projectID=lambda s: 'GEO' + s.projectID.astype(str))
    
    out_df = out_df.query("Country in @countries")

    fuel_cols = {'Fueltype', 'FuelClassification1', 'FuelClassification2'}
    out_df = out_df.replace({col: {'Gas': 'Natural Gas'} for col in fuel_cols})

    out_df = (out_df.pipe(gather_fueltype_info, search_col=['FuelClassification1'])
            .pipe(gather_technology_info, search_col=['FuelClassification1'])
            .pipe(gather_set_info)
            .pipe(config_filter, name='GEO')
            .pipe(clean_name)
            .pipe(clean_technology, generalize_hydros=True)
            .pipe(scale_to_net_capacities, (not CONFIG['GEO']['net_capacity']))
            .pipe(config_filter, name='GEO')
            .pipe(correct_manually, 'GEO')
            )
    
    out_df = out_df.set_index('projectID')
    return out_df # GEO

def GPD(in_df, filter_other_dbs=True):
    """
    Cleaner for the `Global Power Plant Database`.
    if outdated have a look at
    https://datasets.wri.org/dataset/globalpowerplantdatabase

    """
    
    countries = CONFIG['target_countries']
    
    out_df = in_df.copy()
    out_df = out_df.rename(columns=lambda x: x.title())

    if filter_other_dbs:
        other_dbs = ['GEODB', 'CARMA', 'Open Power System Data', 'ENTSOE']
        out_df = out_df.query("Country_Long in @countries & Geolocation_Source not in @other_dbs")
        # .drop(columns='Country')

    out_df.rename(columns={'Name': 'OrigName',
                            'Gppd_Idnr': 'projectID',
                             'Country_Long': 'Country',
                             'Primary_Fuel': 'Fueltype',
                             'Latitude': 'lat',
                             'Longitude': 'lon',
                             'Capacity_Mw': 'Capacity',
                             # 'Source': 'File'
                             'Commissioning_Year': 'DateIn'}, inplace=True)
    # out_df = out_df.reindex( list(out_df['projectID']) )

    out_df['Fueltype'].replace({'Coal': 'Hard Coal',
                                    'Biomass': 'Bioenergy',
                                    'Gas': 'Natural Gas',
                                    'Wave and Tidal': 'Hydro'}, inplace=True)
    
    out_df = (out_df
            .pipe(clean_name)
            .pipe(config_filter, name='GPD')
            # .pipe(gather_technology_info)
            # .pipe(gather_set_info)
            # .pipe(correct_manually, 'GPD')
            )

    out_df = out_df.set_index('projectID')
    return out_df # GPD

def JRC(in_df):
    """
    
    """

    out_df = in_df.copy()
    out_df = (out_df.rename(columns={'id': 'projectID',
                            'name': 'OrigName',
                            'installed_capacity_MW': 'Capacity',
                            'country_code': 'Country',
                            'type': 'Technology',
                            'dam_height_m': 'DamHeight_m',
                            'volume_Mm3': 'Volume_Mm3',
                            'storage_capacity_MWh': 'StorageCapacity_MWh'}))

    out_df = out_df.eval('Duration = StorageCapacity_MWh / Capacity')

    new_tech = out_df['Technology'].replace({'HDAM': 'Reservoir',
                                'HPHS': 'Pumped Storage',
                                'HROR': 'Run-Of-River'})
    out_df.Technology = new_tech

    out_df = out_df.drop(columns=['pypsa_id', 'GEO'])

    out_df = out_df.assign(Set='Store')
    out_df = out_df.assign(Fueltype='Hydro')
        
    out_df = (out_df
                .pipe(convert_alpha2_to_country)
                .pipe(clean_name)
                .pipe(config_filter)
                )

    # TODO: Temporary section to deal with duplicate identifiers in the JRC
    # input file. Can be removed again, once the duplicates have been removed
    # in a new release.
    mask = out_df.projectID.duplicated(keep=False)
    out_df.loc[mask, 'projectID'] += (out_df.groupby('projectID').cumcount()
                                    .replace({0: 'a', 1: 'b', 2: 'c', 3: 'd'}))

    out_df = out_df.set_index('projectID')
    return out_df # JRC

def OPSD_DE(in_df):
    """
    """    
    out_df = in_df.copy()
    # 1. Title case column labels
    out_df.rename(columns=str.title, inplace=True)

    # 2. Rename columns
    col_rename_dict = {'Name_Bnetza': 'OrigName',
                                    'Lat': 'lat',
                                       'Lon': 'lon',
                                       'Fuel': 'Fueltype',
                                       'Type': 'Set',
                                       'Country_Code': 'Country',
                                       'Capacity_Net_Bnetza': 'Capacity',
                                       'Commissioned': 'DateIn',
                                       'Shutdown': 'DateOut',
                                       'Eic_Code_Plant': 'EIC',
                                       'Id': 'projectID'}
    out_df.rename(columns=col_rename_dict, inplace=True)

    # 3A. fillna work on Name based on Name_Bnetza, in turn filled with Name_Uba
    out_df = out_df.assign(OrigName=lambda d: d.OrigName.fillna(d.Name_Uba), inplace=True)

    # 3B. fillna on Fueltype based on Energy_Source_Level column value
    out_df = out_df.assign(Fueltype=lambda d: d.Fueltype.fillna(d.Energy_Source_Level_1))

    # 3C. fillna on Dateretrofit simply by assigning DateIn
    out_df = out_df.assign(DateRetrofit = lambda d: d.Retrofit.fillna(d.DateIn))

    # 4.  Looks like setting Status column to True / False if matches any of the words
    #       Then slice is taken based on this boolean check
    statusDE = ['operating', 'reserve', 'special_case', 'shutdown_temporary']
    out_df = out_df.loc[ out_df['Status'].isin(statusDE) ]

    # 5. Reindex on 'target columns'
    out_df = out_df.reindex(columns=CONFIG['target_columns'])

    # 6. Relabel dimension values for Fueltype based on dictionary
    fuel_rename_dict = {'Biomass and biogas': 'Bioenergy',
                                    'Fossil fuels': np.nan,
                                    'Mixed fossil fuels': 'Other',
                                    'Natural gas': 'Natural Gas',
                                    'Non-renewable waste': 'Waste',
                                    'Other bioenergy and renewable waste':
                                        'Bioenergy',
                                    'Other or unspecified energy sources':
                                        'Other',
                                    'Other fossil fuels': 'Other',
                                    'Other fuels': 'Other'}
    new_fuel = out_df['Fueltype'].replace(fuel_rename_dict)
    out_df['Fueltype'].replace(fuel_rename_dict, inplace=True)

    out_df = out_df.reindex(columns=CONFIG['target_columns'])
    
    out_df['Set'].replace({'IPP': 'PP'}, inplace=True)
    # out_df = out_df['Country'].replace({'UK': u'GB', '[ \t]+|[ \t]+$.': ''}, regex=True) -- not appliable to DE subset

    out_df['Capacity'].replace({0.: np.nan}, regex=True, inplace=True)
    out_df = out_df.dropna(subset=['Capacity'])

    # out_df = out_df.assign(Name=lambda df: df.Name.str.title().str.strip() )

    out_df = out_df.assign(Fueltype=lambda df: df.Fueltype.str.title().str.strip())

    out_df = (out_df
                .pipe(convert_alpha2_to_country)
                .pipe(correct_manually, 'OPSD_DE')
                .pipe(config_filter, name='OPSD_DE')
                .pipe(gather_set_info)
                .pipe(clean_name)
                # .pipe(clean_technology)
                )
    
    out_df = out_df.set_index('projectID')
    return out_df # OPSD_DE

def OPSD_EU(in_df):
    """
    Importer for the OPSD (Open Power Systems Data) database.

    Parameters
    ----------
    rawEU : Boolean, default False
        Whether to return the raw EU (=non-DE) database.
    rawDE : Boolean, default False
        Whether to return the raw DE database.
    statusDE : list, default ['operating', 'reserve', 'special_case']
        Filter DE entries by operational status ['operating', 'shutdown',
        'reserve', etc.]
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    
    out_df = in_df.copy()
    out_df = (out_df.rename(columns=str.title)
               .rename(columns={'Name': 'OrigName',
                                'Lat': 'lat',
                                'Lon': 'lon',
                                'Energy_Source': 'Fueltype',
                                'Commissioned': 'DateIn',
                                'Eic_Code': 'EIC'})
               .eval('DateRetrofit = DateIn')
               .assign(projectID=lambda s: 'OEU'
                       + pd.Series(s.index.astype(str), s.index))
               .reindex(columns=CONFIG['target_columns']))

    fuel_rename_dict = {'Biomass and biogas': 'Bioenergy',
                                    'Fossil fuels': np.nan,
                                    'Mixed fossil fuels': 'Other',
                                    'Natural gas': 'Natural Gas',
                                    'Non-renewable waste': 'Waste',
                                    'Other bioenergy and renewable waste':
                                        'Bioenergy',
                                    'Other or unspecified energy sources':
                                        'Other',
                                    'Other fossil fuels': 'Other',
                                    'Other fuels': 'Other'}

    out_df['Fueltype'].replace(fuel_rename_dict, inplace=True)
    # out_df['Set'].replace({'IPP': 'PP'}, inplace=True) - Appliable to DE subset only?
    out_df['Country'].replace({'UK': u'GB', '[ \t]+|[ \t]+$.': ''}, regex=True, inplace=True)

    out_df['Capacity'].replace({0.: np.nan}, regex=True, inplace=True)
    out_df = out_df.dropna(subset=['Capacity'])

    out_df = out_df.assign(OrigName=lambda df: df.OrigName.str.title().str.strip() )

    out_df = out_df.assign(Fueltype=lambda df: df.Fueltype.str.title().str.strip())

    out_df = (out_df
                .pipe(convert_alpha2_to_country)
                .pipe(correct_manually, 'OPSD_EU')
                .pipe(config_filter, name='OPSD_EU')
                .pipe(gather_set_info)
                .pipe(clean_name)
                .pipe(clean_technology)
                )

    out_df = out_df.set_index('projectID')
    return out_df # OPSD_EU


#=======================================

def OSM():
#    """
#    Parser and Importer for Open Street Map power plant data.
#    """
#    import requests
#    overpass_url = "http://overpass-api.de/api/interpreter"
#    overpass_query = """
#    [out:json][timeout:210];
#    area["name"="Luxembourg"]->.boundaryarea;
#    (
#    // query part for: “power=plant”
#    node["power"="plant"](area.boundaryarea);
#    way["power"="plant"](area.boundaryarea);
#    relation["power"="plant"](area.boundaryarea);
#    node["power"="generator"](area.boundaryarea);
#    way["power"="generator"](area.boundaryarea);
#    relation["power"="generator"](area.boundaryarea);
#    );
#    out body;
#    """
#    response = requests.get(overpass_url,
#                            params={'data': overpass_query})
#    data = response.json()
#    df = pd.DataFrame(data['elements'])
#    df = pd.concat([df.drop(columns='tags'), df.tags.apply(pd.Series)], axis=1)
#
    pass
    return out_df

def WEPP(df=None, raw=False):
    """
    Importer for the standardized WEPP (Platts, World Elecrtric Power
    Plants Database). This database is not provided by this repository because
    of its restrictive licence.

    Parameters
    ----------
    raw : Boolean, default False
        Whether to return the original dataset
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()

    """
    
    # Define the appropriate datatype for each column (some columns e.g.
    # 'YEAR' cannot be integers, as there are N/A values, which np.int
    # does not yet(?) support.)
    datatypes = {'UNIT': str, 'PLANT': str, 'COMPANY': str, 'MW': np.float64,
                 'STATUS': str, 'YEAR': np.float64, 'UTYPE': str, 'FUEL': str,
                 'FUELTYPE': str, 'ALTFUEL': str, 'SSSMFR': str,
                 'BOILTYPE': str, 'TURBMFR': str, 'TURBTYPE': str,
                 'GENMFR': str, 'GENTYPE': str, 'SFLOW': np.float64,
                 'SPRESS': np.float64, 'STYPE': str, 'STEMP': np.float64,
                 'REHEAT1': np.float64, 'REHEAT2': np.float64, 'PARTCTL': str,
                 'PARTMFR': str, 'SO2CTL': str, 'FGDMFR': str, 'NOXCTL': str,
                 'NOXMFR': str, 'AE': str, 'CONstr, UCT': str, 'COOL': str,
                 'RETIRE': np.float64, 'CITY': str, 'STATE': str,
                 'COUNTRY': str, 'AREA': str, 'SUBREGION': str,
                 'POSTCODE': str, 'PARENT': str, 'ELECTYPE': str,
                 'BUSTYPE': str, 'COMPID': str, 'LOCATIONID': str,
                 'UNITID': str}
    # Now read the Platts WEPP Database
    wepp = pd.read_csv(CONFIG['WEPP']['source_file'], dtype=datatypes,
                       encoding='utf-8')
    if raw:
        return wepp

    # Fit WEPP-column names to our specifications
    wepp.columns = wepp.columns.str.title()
    wepp.rename(columns={'Unit': 'Name',
                         'Fuel': 'Fueltype',
                         'Fueltype': 'Technology',
                         'Mw': 'Capacity',
                         'Year': 'DateIn',
                         'Retire': 'DateOut',
                         'Lat': 'lat',
                         'Lon': 'lon',
                         'Unitid': 'projectID'}, inplace=True)
    wepp.loc[:, 'DateRetrofit'] = wepp.DateIn
    # Do country transformations and drop those which are not in definded scope
    c = {'ENGLAND & WALES': u'UNITED KINGDOM',
         'GIBRALTAR': u'SPAIN',
         'SCOTLAND': u'UNITED KINGDOM'}
    wepp.Country = wepp.Country.replace(c).str.title()
    wepp = (wepp.loc[lambda df: df.Country.isin(config['target_countries'])]
                .loc[lambda df: df.Status.isin(['OPR', 'CON'])]
                .assign(File=config['WEPP']['source_file']))
    # Replace fueltypes
    d = {'AGAS': 'Bioenergy',    # Syngas from gasified agricultural waste
         'BFG': 'Other',         # blast furnance gas -> "Hochofengas"
         'BGAS': 'Bioenergy',
         'BIOMASS': 'Bioenergy',
         'BL': 'Bioenergy',
         'CGAS': 'Hard Coal',
         'COAL': 'Hard Coal',
         'COG': 'Other',         # coke oven gas -> deutsch: "Hochofengas"
         'COKE': 'Hard Coal',
         'CSGAS': 'Hard Coal',   # Coal-seam-gas
         'CWM': 'Hard Coal',     # Coal-water mixture (aka coal-water slurry)
         'DGAS': 'Other',        # sewage digester gas -> deutsch: "Klaergas"
         'FGAS': 'Other',        # Flare gas or wellhead gas or associated gas
         'GAS': 'Natural Gas',
         'GEO': 'Geothermal',
         'H2': 'Other',          # Hydrogen gas
         'HZDWST': 'Waste',      # Hazardous waste
         'INDWST': 'Waste',      # Industrial waste or refinery waste
         'JET': 'Oil',           # Jet fuels
         'KERO': 'Oil',          # Kerosene
         'LGAS': 'Other',        # landfill gas -> deutsch: "Deponiegas"
         'LIGNIN': 'Bioenergy',
         'LIQ': 'Other',         # (black) liqour -> deutsch: "Schwarzlauge",
                                 #    die bei Papierherstellung anfaellt
         'LNG': 'Natural Gas',   # Liquified natural gas
         'LPG': 'Natural Gas',   # Liquified petroleum gas (u. butane/propane)
         'MBM': 'Bioenergy',     # Meat and bonemeal
         'MEDWST': 'Bioenergy',  # Medical waste
         'MGAS': 'Other',        # mine gas -> deutsch: "Grubengas"
         'NAP': 'Oil',           # naphta
         'OGAS': 'Oil',          # Gasified crude oil/refinery bottoms/bitumen
         'PEAT': 'Other',
         'REF': 'Waste',
         'REFGAS': 'Other',      # Syngas from gasified refuse
         'RPF': 'Waste',         # Waste paper and/or waste plastic
         'PWST': 'Other',        # paper mill waste
         'RGAS': 'Other',        # refinery off-gas -> deutsch: "Raffineriegas"
         'SHALE': 'Oil',
         'SUN': 'Solar',
         'TGAS': 'Other',        # top gas -> deutsch: "Hochofengas"
         'TIRES': 'Other',       # Scrap tires
         'UNK': 'Other',
         'UR': 'Nuclear',
         'WAT': 'Hydro',
         'WOOD': 'Bioenergy',
         'WOODGAS': 'Bioenergy',
         'WSTGAS': 'Other',      # waste gas -> deutsch: "Industrieabgas"
         'WSTWSL': 'Waste',      # Wastewater sludge
         'WSTH': 'Waste'}
    wepp.Fueltype = wepp.Fueltype.replace(d)
    # Fill NaNs to allow str actions
    wepp.Technology.fillna('', inplace=True)
    wepp.Turbtype.fillna('', inplace=True)
    # Correct technology infos:
    wepp.loc[wepp.Technology.str.contains('LIG', case=False),
             'Fueltype'] = 'Lignite'
    wepp.loc[wepp.Turbtype.str.contains('KAPLAN|BULB', case=False),
             'Technology'] = 'Run-Of-River'
    wepp.Technology = wepp.Technology.replace({'CONV/PS': 'Pumped Storage',
                                               'CONV': 'Reservoir',
                                               'PS': 'Pumped Storage'})
    tech_st_pattern = ['ANTH', 'BINARY', 'BIT', 'BIT/ANTH', 'BIT/LIG',
                       'BIT/SUB', 'BIT/SUB/LIG', 'COL', 'DRY ST', 'HFO', 'LIG',
                       'LIG/BIT', 'PWR', 'RDF', 'SUB']
    tech_ocgt_pattern = ['AGWST', 'LITTER', 'RESID', 'RICE', 'STRAW']
    tech_ccgt_pattern = ['LFO']
    wepp.loc[wepp.Technology.isin(tech_st_pattern),
             'Technology'] = 'Steam Turbine'
    wepp.loc[wepp.Technology.isin(tech_ocgt_pattern), 'Technology'] = 'OCGT'
    wepp.loc[wepp.Technology.isin(tech_ccgt_pattern), 'Technology'] = 'CCGT'
    ut_ccgt_pattern = ['CC', 'GT/C', 'GT/CP', 'GT/CS', 'GT/ST', 'ST/C',
                       'ST/CC/GT', 'ST/CD', 'ST/CP', 'ST/CS', 'ST/GT',
                       'ST/GT/IC', 'ST/T', 'IC/CD', 'IC/CP', 'IC/GT']
    ut_ocgt_pattern = ['GT', 'GT/D', 'GT/H', 'GT/HY', 'GT/IC', 'GT/S', 'GT/T',
                       'GTC']
    ut_st_pattern = ['ST', 'ST/D']
    ut_ic_pattern = ['IC', 'IC/H']
    wepp.loc[wepp.Utype.isin(ut_ccgt_pattern), 'Technology'] = 'CCGT'
    wepp.loc[wepp.Utype.isin(ut_ocgt_pattern), 'Technology'] = 'OCGT'
    wepp.loc[wepp.Utype.isin(ut_st_pattern), 'Technology'] = 'Steam Turbine'
    wepp.loc[wepp.Utype.isin(ut_ic_pattern),
             'Technology'] = 'Combustion Engine'
    wepp.loc[wepp.Utype == 'WTG', 'Technology'] = 'Onshore'
    wepp.loc[wepp.Utype == 'WTG/O', 'Technology'] = 'Offshore'
    wepp.loc[(wepp.Fueltype == 'Solar') & (wepp.Utype.isin(ut_st_pattern)),
             'Technology'] = 'CSP'
    # Derive the SET column
    chp_pattern = ['CC/S', 'CC/CP', 'CCSS/P', 'GT/CP', 'GT/CS', 'GT/S', 'GT/H',
                   'IC/CP', 'IC/H', 'ST/S', 'ST/H', 'ST/CP', 'ST/CS', 'ST/D']
    wepp.loc[wepp.Utype.isin(chp_pattern), 'Set'] = 'CHP'
    wepp.loc[wepp.Set.isnull(), 'Set'] = 'PP'
    # Clean up the mess
    wepp.Fueltype = wepp.Fueltype.str.title()
    wepp.loc[wepp.Technology.str.len() > 4, 'Technology'] = \
        wepp.loc[wepp.Technology.str.len() > 4, 'Technology'].str.title()
    # Done!
    wepp.datasetID = 'WEPP'
    return (wepp
            .pipe(config_filter, name='WEPP', config=config)
            # .pipe(scale_to_net_capacities, (not config['WEPP']['net_capacity']))
            .pipe(correct_manually, 'WEPP', config=config))

def UBA(df=None, header=9, skipfooter=26, prune_wind=True, prune_solar=True,
        update=False, raw=False):
    """
    Importer for the UBA Database. Please download the data from
    ``https://www.umweltbundesamt.de/dokument/datenbank-kraftwerke-in
    -deutschland`` and place it in ``powerplantmatching/data/in``.

    Parameters:
    -----------
    header : int, Default 9
        The zero-indexed row in which the column headings are found.
    skipfooter : int, Default 26
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()

    """
    
    def parse_func(url): return pd.read_excel(url, skipfooter=skipfooter,
                                              na_values='n.b.', header=header)
    uba = parse_if_not_stored('UBA', update, parse_func)
    if raw:
        return uba
    uba = uba.rename(columns={
        u'Kraftwerksname / Standort': 'Name',
        u'Elektrische Bruttoleistung (MW)': 'Capacity',
        u'Inbetriebnahme  (ggf. Ertüchtigung)': 'DateIn',
        u'Primärenergieträger': 'Fueltype',
        u'Anlagenart': 'Technology',
        u'Fernwärme-leistung (MW)': 'CHP',
        u'Standort-PLZ': 'PLZ'})
    from .heuristics import PLZ_to_LatLon_map
    uba = (uba.assign(
        Name=uba.Name.replace({'\s\s+': ' '}, regex=True),
        lon=uba.PLZ.map(PLZ_to_LatLon_map()['lon']),
        lat=uba.PLZ.map(PLZ_to_LatLon_map()['lat']),
        DateIn=uba.DateIn.str.replace(
            "\(|\)|\/|\-", " ").str.split(' ').str[0].astype(float),
        Country='Germany',
        File='kraftwerke-de-ab-100-mw.xls',
        projectID=['UBA{:03d}'.format(i + header + 2) for i in uba.index],
        Technology=uba.Technology.replace({
            u'DKW': 'Steam Turbine',
            u'DWR': 'Pressurized Water Reactor',
            u'G/AK': 'Steam Turbine',
            u'GT': 'OCGT',
            u'GuD': 'CCGT',
            u'GuD / HKW': 'CCGT',
            u'HKW': 'Steam Turbine',
            u'HKW (DT)': 'Steam Turbine',
            u'HKW / GuD': 'CCGT',
            u'HKW / SSA': 'Steam Turbine',
            u'IKW': 'OCGT',
            u'IKW / GuD': 'CCGT',
            u'IKW / HKW': 'Steam Turbine',
            u'IKW / HKW / GuD': 'CCGT',
            u'IKW / SSA': 'OCGT',
            u'IKW /GuD': 'CCGT',
            u'LWK': 'Run-Of-River',
            u'PSW': 'Pumped Storage',
            u'SWK': 'Reservoir Storage',
            u'SWR': 'Boiled Water Reactor'})))
    uba.loc[uba.CHP.notnull(), 'Set'] = 'CHP'
    uba = uba.pipe(gather_set_info)
    uba.loc[uba.Fueltype == 'Wind (O)', 'Technology'] = 'Offshore'
    uba.loc[uba.Fueltype == 'Wind (L)', 'Technology'] = 'Onshore'
    uba.loc[uba.Fueltype.str.contains('Wind'), 'Fueltype'] = 'Wind'
    uba.loc[uba.Fueltype.str.contains('Braunkohle'), 'Fueltype'] = 'Lignite'
    uba.loc[uba.Fueltype.str.contains('Steinkohle'), 'Fueltype'] = 'Hard Coal'
    uba.loc[uba.Fueltype.str.contains('Erdgas'), 'Fueltype'] = 'Natural Gas'
    uba.loc[uba.Fueltype.str.contains('HEL'), 'Fueltype'] = 'Oil'
    uba.Fueltype = uba.Fueltype.replace({u'Biomasse': 'Bioenergy',
                                         u'Gichtgas': 'Other',
                                         u'HS': 'Oil',
                                         u'Konvertergas': 'Other',
                                         u'Licht': 'Solar',
                                         u'Raffineriegas': 'Other',
                                         u'Uran': 'Nuclear',
                                         u'Wasser': 'Hydro',
                                         u'\xd6lr\xfcckstand': 'Oil'})
    uba.Name.replace([r'(?i)oe', r'(?i)ue'], [u'ö', u'ü'], regex=True,
                     inplace=True)
    if prune_wind:
        uba = uba.loc[lambda x: x.Fueltype != 'Wind']
    if prune_solar:
        uba = uba.loc[lambda x: x.Fueltype != 'Solar']
    return (uba
            # .pipe(scale_to_net_capacities, not config['UBA']['net_capacity'])
            # .pipe(config_filter, name='UBA', config=config)
            # .pipe(correct_manually, 'UBA', config=config)
            )

def BNETZA(df=None, header=9, sheet_name='Gesamtkraftwerksliste BNetzA',
           prune_wind=True, prune_solar=True, raw=False, update=False,
           config=None):
    """
    Importer for the database put together by Germany's 'Federal Network
    Agency' (dt. 'Bundesnetzagentur' (BNetzA)).
    Please download the data from
    ``https://www.bundesnetzagentur.de/DE/Sachgebiete/ElektrizitaetundGas/
    Unternehmen_Institutionen/Versorgungssicherheit/Erzeugungskapazitaeten/
    Kraftwerksliste/kraftwerksliste-node.html``
    and place it in ``powerplantmatching/data/in``.

    Parameters:
    -----------
    header : int, Default 9
        The zero-indexed row in which the column headings are found.
    raw : Boolean, default False
        Whether to return the original dataset
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    config = _get_config() if config is None else config

    url = config['BNETZA']['url']

    def parse_func():
        return pd.read_excel(url, header=header, sheet_name=sheet_name,
                             parse_dates=False)
    bnetza = parse_if_not_stored('BNETZA', update, config, parse_func)

    if raw:
        return bnetza
    bnetza = bnetza.rename(columns={
        'Kraftwerksnummer Bundesnetzagentur': 'projectID',
        'Kraftwerksname': 'Name',
        'Netto-Nennleistung (elektrische Wirkleistung) in MW': 'Capacity',
        'Wärmeauskopplung (KWK)\n(ja/nein)': 'Set',
        'Ort\n(Standort Kraftwerk)': 'Ort',
        ('Auswertung\nEnergieträger (Zuordnung zu einem '
         'Hauptenergieträger bei Mehreren Energieträgern)'): 'Fueltype',
        'Kraftwerksstatus \n(in Betrieb/\nvorläufig '
        'stillgelegt/\nsaisonale Konservierung\nNetzreserve/ '
        'Sicherheitsbereitschaft/\nSonderfall)': 'Status',
        ('Aufnahme der kommerziellen Stromerzeugung der derzeit '
         'in Betrieb befindlichen Erzeugungseinheit\n(Datum/Jahr)'):
        'DateIn',
        'PLZ\n(Standort Kraftwerk)': 'PLZ'})
    # If BNetzA-Name is empty replace by company, if this is empty by city.

    from .heuristics import PLZ_to_LatLon_map

    pattern = '|'.join(['.*(?i)betrieb', '.*(?i)gehindert', '(?i)vorläufig.*',
                        'Sicherheitsbereitschaft', 'Sonderfall'])
    bnetza = (bnetza.assign(
              lon=bnetza.PLZ.map(PLZ_to_LatLon_map()['lon']),
              lat=bnetza.PLZ.map(PLZ_to_LatLon_map()['lat']),
              Name=bnetza.Name.where(bnetza.Name.str.len().fillna(0) > 4,
                                     bnetza.Unternehmen + ' '
                                     + bnetza.Name.fillna(''))
              .fillna(bnetza.Ort).str.strip(),
              DateIn=bnetza.DateIn.str[:4]
              .apply(pd.to_numeric, errors='coerce'),
              Blockname=bnetza.Blockname.replace(
                  {'.*(GT|gasturbine).*': 'OCGT',
                   '.*(DT|HKW|(?i)dampfturbine|(?i)heizkraftwerk).*':
                       'Steam Turbine',
                   '.*GuD.*': 'CCGT'}, regex=True))
              [lambda df: df.projectID.notna()
               & df.Status.str.contains(pattern, regex=True, case=False)]
              .pipe(gather_technology_info,
                    search_col=['Name', 'Fueltype', 'Blockname'],
                    config=config))

    add_location_b = (bnetza[bnetza.Ort.notnull()]
                      .apply(lambda ds: (ds['Ort'] not in ds['Name'])
                             and (str.title(ds['Ort']) not in ds['Name']),
                             axis=1))
    bnetza.loc[bnetza.Ort.notnull() & add_location_b, 'Name'] = (
        bnetza.loc[bnetza.Ort.notnull() & add_location_b, 'Ort']
        + ' '
        + bnetza.loc[bnetza.Ort.notnull() & add_location_b, 'Name'])

    techmap = {'solare': 'PV',
               'Laufwasser': 'Run-Of-River',
               'Speicherwasser': 'Reservoir',
               'Pumpspeicher': 'Pumped Storage'}
    for fuel in techmap:
        bnetza.loc[bnetza.Fueltype.str.contains(fuel, case=False),
                   'Technology'] = techmap[fuel]
    # Fueltypes
    bnetza.Fueltype.replace({'Erdgas': 'Natural Gas',
                             'Steinkohle': 'Hard Coal',
                             'Braunkohle': 'Lignite',
                             'Wind.*': 'Wind',
                             'Solar.*': 'Solar',
                             '.*(?i)energietr.*ger.*\n.*': 'Other',
                             'Kern.*': 'Nuclear',
                             'Mineral.l.*': 'Oil',
                             'Biom.*': 'Bioenergy',
                             '.*(?i)(e|r|n)gas': 'Other',
                             'Geoth.*': 'Geothermal',
                             'Abfall': 'Waste',
                             '.*wasser.*': 'Hydro',
                             '.*solar.*': 'PV'},
                            regex=True, inplace=True)
    if prune_wind:
        bnetza = bnetza[lambda x: x.Fueltype != 'Wind']
    if prune_solar:
        bnetza = bnetza[lambda x: x.Fueltype != 'Solar']
    # Filter by country
    bnetza = bnetza[~bnetza.Bundesland.isin([u'Österreich', 'Schweiz',
                                             'Luxemburg'])]
    return (bnetza.assign(Country='Germany',
                          Set=bnetza.Set.fillna('Nein').str.title()
                          .replace({'Ja': 'CHP', 'Nein': 'PP'}))
            # .pipe(config_filter, name='BNETZA', config=config)
            # .pipe(correct_manually, 'BNETZA', config=config)
            )

def OPSD_VRE(df=None, config=None, raw=False):
    """
    Importer for the OPSD (Open Power Systems Data) renewables (VRE)
    database.

    This sqlite database is very big and hence not part of the package.
    It needs to be obtained here:
        http://data.open-power-system-data.org/renewable_power_plants/

    Parameters
    ----------
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    config = _get_config() if config is None else config

    df = parse_if_not_stored('OPSD_VRE', index_col=0, low_memory=False)
    if raw:
        return df

    return (df.rename(columns={'energy_source_level_2': 'Fueltype',
                              'technology': 'Technology',
                              'data_source': 'file',
                              'country': 'Country',
                              'electrical_capacity': 'Capacity',
                              'municipality': 'Name'})
        .assign(DateIn=lambda df:
                df.commissioning_date.str[:4].astype(float),
                Set='PP')\
        .powerplant.convert_alpha2_to_country()
        .pipe(config_filter, config=config)
        .drop('Name', axis=1)
        )

def OPSD_VRE_country(country, config=None, raw=False):
    """
    Get country specifig data from OPSD for renewables, if available.
    Available for DE, FR, PL, CH, DK, CZ and SE (last update: 09/2020).
    """
    config = _get_config() if config is None else config

    #there is a problem with GB in line 1651 (version 20/08/20) use low_memory
    df = parse_if_not_stored(f'OPSD_VRE_{country}', low_memory=False)
    if raw:
        return df

    return (df.assign(Country=country, Set='PP')
              .rename(columns={'energy_source_level_2': 'Fueltype',
                               'technology': 'Technology',
                               'data_source': 'file',
                               'electrical_capacity': 'Capacity',
                               'municipality': 'Name'})
              #there is a problem with GB in line 1651 (version 20/08/20)
              .assign(Capacity = lambda df: pd.to_numeric(df.Capacity, 'coerce'))
              .powerplant.convert_alpha2_to_country()
              .pipe(config_filter, config=config)
              .drop('Name', axis=1))
