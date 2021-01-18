"""
Utility functions for checking data completness and supporting other functions
"""

import os
import pandas as pd

from _globals import CONFIG, COUNTRY_MAP, SUB_LAND
import _globals as glob

from core import logger

def get_data(df, idx_val):

    target_cols = CONFIG['target_columns']
    if 'projectID' in target_cols:
        target_cols.remove('projectID')

    # idx_df = df.set_index('projectID')
    # print(df.index)

    missing_ids_dict = {}
    missing_ids_dict['ENTSOE'] = ['50WP00000000707U']
    missing_ids_dict['JRC'] = ['H70', 'H199', 'H223', 'H573', 'H1495', 'H1554', 'H2499', 'H2501', 'H37', 'H2877', 'H2565', 'H3189']
    missing_ids_dict['CARMA'] = []
    
    missing_ids = missing_ids_dict['ENTSOE'] + missing_ids_dict['JRC']

    if idx_val not in missing_ids:
        data = df.loc[idx_val, target_cols]
    else:
        data = pd.Series()

    return data

def convert_to_num(elem):

    if type(elem) == str:
        no_comma = elem.replace(",", "")
        num = float(no_comma)
    else:
        num = elem

    return num

def config_filter(in_df, name=None):

    """
    Convenience function to filter data source according to the config.yaml
    file. Individual query filters are applied if argument 'name' is given.

    Parameters
    ----------
    df : pd.DataFrame
        Data to be filtered
    name : str, default None
        Name of the data source to identify query in the config.yaml file
    config : dict, default None
        Configuration overrides varying from the config.yaml file
    """

    # individual filter from config.yaml
    out_df = in_df.copy()
    if name is not None:
        queries = {k: v for source in CONFIG['matching_sources']
                   for k, v in to_dict_if_string(source).items()}
    
        if name in queries and queries[name] is not None:
            out_df = out_df.query(queries[name])
    
    countries = CONFIG['target_countries']
    fueltypes = CONFIG['target_fueltypes']

    out_df = out_df.query("Country in @countries and Fueltype in @fueltypes")
    out_df = out_df.reindex(columns=CONFIG['target_columns'])
    out_df = out_df.reset_index(drop=True)
    
    return out_df

def convert_alpha2_to_country(df):
    # df = get_obj_if_Acc(df)
    dic = {'EL': 'GR',  # needed, as some datasets use for Greece and United K.
           'UK': 'GB'}  # codes that are not conform to ISO 3166-1 alpha2.
    return df.assign(Country=df.Country.replace(dic)
                     .map(COUNTRY_MAP.set_index('alpha_2')['name']))

def correct_manually(df, name):
    """
    Update powerplant data based on stored corrections in
    powerplantmatching/data/in/manual_corrections.csv. Specify the name
    of the data by the second argument.

    Parameters
    ----------
    df : pandas.DataFrame
        Powerplant data
    name : str
        Name of the data source, should be in columns of manual_corrections.csv
    """
    
    corrections_fn = glob.package_data('manual_corrections.csv')
    corrections = pd.read_csv(corrections_fn)

    corrections = (corrections.query('Source == @name')
                   .drop(columns='Source').set_index('projectID'))
    if corrections.empty:
        return df

    df = df.set_index('projectID').copy()
    df.update(corrections)
    return df.reset_index().reindex(columns=CONFIG['target_columns'])

def fill_geoposition(df, use_saved_locations=False, saved_only=False):
    """
    Fill missing 'lat' and 'lon' values. Uses geoparsing with the value given
    in 'Name', limits the search through value in 'Country'.
    df must contain 'Name', 'lat', 'lon' and 'Country' as columns.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame of power plants
    use_saved_postion : Boolean, default False
        Whether to firstly compare with cached results in
        powerplantmatching/data/parsed_locations.csv
    """
    # df = get_obj_if_Acc(df)

    if use_saved_locations and CONFIG['google_api_key'] is None:
        logger.warning('Geoparsing not possible as no google api key was '
                       'found, please add the key to your config.yaml if you '
                       'want to enable it.')

    if use_saved_locations:
        locs = pd.read_csv(glob.package_data(
            'parsed_locations.csv'), index_col=[0, 1])
        df = df.where(df[['lat', 'lon']].notnull().all(1),
                      df.drop(columns=['lat', 'lon'])
                      .join(locs, on=['PlantName', 'Country']))
    if saved_only:
        return df

    logger.info("Parse geopositions for missing lat/lon values")
    missing = df.lat.isnull()
    geodata = df[missing].apply(
        lambda ds: parse_Geoposition(ds['Name'], country=ds['Country']),
        axis=1)
    geodata.drop_duplicates(subset=['Name', 'Country'])\
           .set_index(['Name', 'Country'])\
           .to_csv(glob.package_data('parsed_locations.csv'), mode='a', header=False)

    df.loc[missing, ['lat', 'lon']] = geodata

    return df.reindex(columns=df.columns)

def get_name(df):
    """
    Helper function to associate dataframe with a name. This is done with the
    columns-axis name, as pd.DataFrame do not have a name attribute.
    """
    if df.columns.name is None:
        return 'unnamed data'
    else:
        return df.columns.name

def lookup(df, keys=None, by='Country, Fueltype', exclude=None, unit='MW'):

    """
    Returns a lookup table of the dataframe df with rounded numbers.
    Use different lookups as "Country", "Fueltype" for the different lookups.

    Parameters
    ----------
    df : pandas.Dataframe or list of pandas.Dataframe's
        powerplant databases to be analysed. If multiple dataframes are passed
        the lookup table will display them in a MulitIndex
    by : string out of 'Country, Fueltype', 'Country' or 'Fueltype'
        Define the type of lookup table you want to obtain.
    keys : list of strings
        labels of the different datasets, only necessary if multiple dataframes
        passed
    exclude: list
        list of fueltype to exclude from the analysis
    """

    # df = get_obj_if_Acc(df)
    if unit == 'GW':
        scaling = 1000.
    elif unit == 'MW':
        scaling = 1.
    else:
        raise(ValueError("unit has to be MW or GW"))

    def lookup_single(df, by=by, exclude=exclude):
        df = read_csv_if_string(df)
        if isinstance(by, str):
            by = by.replace(' ', '').split(',')
        if exclude is not None:
            df = df[~df.Fueltype.isin(exclude)]
        return df.groupby(by).Capacity.sum()

    if isinstance(df, list):
        if keys is None:
            keys = [get_name(d) for d in df]
        dfs = pd.concat([lookup_single(a) for a in df], axis=1,
                        keys=keys, sort=False)
        dfs = dfs.fillna(0.)
        return (dfs/scaling).round(3)
    else:
        return (lookup_single(df)/scaling).fillna(0.).round(3)

def parmap(f, arg_list):
    """
    Parallel mapping function. Use this function to parallely map function
    f onto arguments in arg_list. The maximum number of parallel threads is
    taken from config.yaml:parallel_duke_processes.

    Paramters
    ---------

    f : function
        python funtion with one argument
    arg_list : list
        list of arguments mapped to f
    """

    if CONFIG['parallel_duke_processes']:
        nprocs = min(multiprocessing.cpu_count(), CONFIG['process_limit'])
        logger.info('Run process with {} parallel threads.'.format(nprocs))
        q_in = multiprocessing.Queue(1)
        q_out = multiprocessing.Queue()

        proc = [multiprocessing.Process(target=fun, args=(f, q_in, q_out))
                for _ in range(nprocs)]
        for p in proc:
            p.daemon = True
            p.start()

        sent = [q_in.put((i, x)) for i, x in enumerate(arg_list)]
        [q_in.put((None, None)) for _ in range(nprocs)]
        res = [q_out.get() for _ in range(len(sent))]

        [p.join() for p in proc]

        return [x for i, x in sorted(res)]

    else:
        return list(map(f, arg_list))

def parse_Geoposition(location, zipcode='', country='', use_saved_locations=False, saved_only=False):
    """
    Nominatim request for the Geoposition of a specific location in a country.
    Returns a tuples with (latitude, longitude, country) if the request was
    sucessful, returns np.nan otherwise.

    ToDo:   There exist further online sources for lat/long data which could be
            used, if this one fails, e.g.
        - Google Geocoding API
        - Yahoo! Placefinder
        - https://askgeo.com (??)

    Parameters
    ----------
    location : string
        description of the location, can be city, area etc.
    country : string
        name of the country which will be used as a bounding area
    use_saved_postion : Boolean, default False
        Whether to firstly compare with cached results in
        powerplantmatching/data/parsed_locations.csv
    """

    from geopy.geocoders import GoogleV3  # ArcGIS  Yandex Nominatim
    import geopy.exc

    if location is None or location == float:
        return np.nan

    alpha2 = country_alpha2(country)
    try:
        gdata = (GoogleV3(api_key=CONFIG['google_api_key'], timeout=10)
                 .geocode(query=location,
                          components={'country': alpha2,
                                      'postal_code': str(zipcode)},
                          exactly_one=True))
    except geopy.exc.GeocoderQueryError as e:
        logger.warn(e)

    if gdata is not None:
        return pd.Series({'Name': location, 'Country': country,
                          'lat': gdata.latitude, 'lon': gdata.longitude})

"""
def parse_if_not_stored(name, update=False, parse_func=None, **kwargs):
    df_config = CONFIG[name]
    path = _set_path(df_config['fn'], SUB_LAND)

    if not os.path.exists(path) or update:
        if parse_func is None:
            df_url = df_config['url']
            logger.info(f'Retrieving data from {df_url}')
            data = pd.read_csv(df_config['url'], **kwargs)
        else:
            data = parse_func()
        data.to_csv(path)
    else:
        data = pd.read_csv(path, **kwargs)
    return data
"""

def projectID_to_dict(df):
    """
    Convenience function to convert string of dict to dict type
    """
    if df.columns.nlevels > 1:
        return df.assign(projectID=(df.projectID.stack().dropna().apply(
            lambda ds: liteval(ds)).unstack()))
    else:
        return df.assign(projectID=df.projectID.apply(lambda x: liteval(x)))

def read_csv_if_string(df):
    """
    Convenience function to import powerplant data source if a string is given.
    """
    import data
    if isinstance(data, six.string_types):
        df = getattr(data, df)()
    return df

def set_uncommon_fueltypes_to_other(df, fillna_other=True, **kwargs):
   
    """
    Replace uncommon fueltype specifications as by 'Other'. This helps to
    compare datasources with Capacity statistics given by
    powerplantmatching.data.Capacity_stats().

    Parameters
    ----------

    df : pd.DataFrame
        DataFrame to replace 'Fueltype' argument
    fillna_other : Boolean, default True
        Whether to replace NaN values in 'Fueltype' with 'Other'
    fueltypes : list
        list of replaced fueltypes, defaults to
        ['Bioenergy', 'Geothermal', 'Mixed fuel types', 'Electro-mechanical',
        'Hydrogen Storage']
    """
    default = ['Bioenergy', 'Geothermal', 'Mixed fuel types',
               'Electro-mechanical', 'Hydrogen Storage']
    fueltypes = kwargs.get('fueltypes', default)
    df.loc[df.Fueltype.isin(fueltypes), 'Fueltype'] = 'Other'
    if fillna_other:
        df = df.fillna({'Fueltype': 'Other'})
    return df

def to_dict_if_string(s):
    """
    Convenience function to ensure dict-like output
    """
    if isinstance(s, str):
        return {s: None}
    else:
        return s