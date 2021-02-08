

def ENTSOE_ORIG(df=None, update=False, raw=False, entsoe_token=None, config=None):
    """
    Importer for the list of installed generators provided by the ENTSO-E
    Trasparency Project. Geographical information is not given.
    If update=True, the dataset is parsed through a request to
    'https://transparency.entsoe.eu/generation/r2/\
    installedCapacityPerProductionUnit/show',
    Internet connection requiered. If raw=True, the same request is done, but
    the unprocessed data is returned.
    Parameters
    ----------
    update : Boolean, Default False
        Whether to update the database through a request to the ENTSO-E
        transparency plattform
    raw : Boolean, Default False
        Whether to return the raw data, obtained from the request to
        the ENTSO-E transparency platform
    entsoe_token: String
        Security token of the ENTSO-E Transparency platform
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    Note: For obtaining a security token refer to section 2 of the
    RESTful API documentation of the ENTSOE-E Transparency platform
    https://transparency.entsoe.eu/content/static_content/Static%20content/
    web%20api/Guide.html#_authentication_and_authorisation. Please save the
    token in your config.yaml file (key 'entsoe_token').
    """
    config = get_config() if config is None else config

    def parse_entsoe():
        assert entsoe_token is not None, "entsoe_token is missing"
        url = 'https://transparency.entsoe.eu/api'
        # retrieved from pd.read_html('https://transparency.entsoe.eu/content/stat
        # ic_content/Static%20content/web%20api/Guide.html#_request_methods')[-1]
        domains = list(entsoe_api.mappings.BIDDING_ZONES.values())

        level1 = ['registeredResource.name', 'registeredResource.mRID']
        level2 = ['voltage_PowerSystemResources.highVoltageLimit', 'psrType']
        level3 = ['quantity']

        def namespace(element):
            m = re.match('\{.*\}', element.tag)
            return m.group(0) if m else ''

        entsoe = pd.DataFrame()
        logger.info(f"Retrieving data from {url}")
        for domain in domains:
            ret = requests.get(url, params=dict(
                securityToken=entsoe_token, documentType='A71',
                processType='A33', In_Domain=domain,
                periodStart='201612312300', periodEnd='201712312300'))
            etree = ET.fromstring(ret.content)
            ns = namespace(etree)
            df_domain = pd.DataFrame(columns=level1+level2+level3+['Country'])
            for i, level in enumerate([level1, level2, level3]):
                for arg in level:
                    df_domain[arg] = [
                        e.text for e in etree.findall('*/' * (i+1) + ns + arg)]
            entsoe = entsoe.append(df_domain, ignore_index=True)
        return entsoe

    if config['entsoe_token'] is not None:
        entsoe_token = config['entsoe_token']
        df = parse_if_not_stored('ENTSOE', update, config, parse_entsoe)
    else:
        if update:
            logger.info('No entsoe_token in config.yaml given, '
                        'falling back to stored version.')
        df = parse_if_not_stored('ENTSOE', update, config)

    if raw:
        return df

    fuelmap = entsoe_api.mappings.PSRTYPE_MAPPINGS
    country_map_entsoe = pd.read_csv(_package_data('entsoe_country_codes.csv'),
                                     index_col=0).rename(index=str).Country
    countries = config['target_countries']

    return (df.rename(columns={'psrType': 'Fueltype',
                               'quantity': 'Capacity',
                               'registeredResource.mRID': 'projectID',
                               'registeredResource.name': 'Name'})
            .reindex(columns=config['target_columns'])
            .replace({'Fueltype': fuelmap})
            .drop_duplicates('projectID')
            .assign(EIC=lambda df: df.projectID,
                    Country=lambda df: df.projectID.str[:2]
                                         .map(country_map_entsoe),
                    Name=lambda df: df.Name.str.title(),
                    Fueltype=lambda df: df.Fueltype.replace(
                        {'Fossil Hard coal': 'Hard Coal',
                         'Fossil Coal-derived gas': 'Other',
                         '.*Hydro.*': 'Hydro',
                         '.*Oil.*': 'Oil',
                         '.*Peat': 'Bioenergy',
                         'Fossil Brown coal/Lignite': 'Lignite',
                         'Biomass': 'Bioenergy',
                         'Fossil Gas': 'Natural Gas',
                         'Marine': 'Other',
                         'Wind Offshore': 'Offshore',
                         'Wind Onshore': 'Onshore'}, regex=True),
                    Capacity=lambda df: pd.to_numeric(df.Capacity))
            .pipe(convert_alpha2_to_country)
            .pipe(clean_powerplantname)
            .pipe(fill_geoposition, use_saved_locations=True, saved_only=True)
            .query('Capacity > 0')
            .pipe(gather_technology_info, config=config)
            .pipe(gather_set_info)
            .pipe(clean_technology)
            .pipe(set_column_name, 'ENTSOE')
            .pipe(config_filter, name='ENTSOE', config=config)
            .pipe(correct_manually, 'ENTSOE', config=config)
            )

            # .powerplant.convert_alpha2_to_country()

def GEO_ORIG(df=None, raw=False, config=None):
    """
    Importer for the GEO database.

    Parameters
    ----------
    raw : Boolean, default False
        Whether to return the original dataset
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    # config = _get_config() if config is None else config

    countries = CONFIG['target_countries']
    rename_cols = {'GEO_Assigned_Identification_Number': 'projectID',
                   'Name': 'Name',
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

    geo = parse_if_not_stored('GEO', config=CONFIG, low_memory=False)
    if raw:
        return geo
    geo = geo.rename(columns=rename_cols)
    
    units = parse_if_not_stored('GEO_units', config=CONFIG, low_memory=False)

    # map from units to plants
    units['DateIn'] = units.Date_Commissioned_dt.str[:4].astype(float)
    units['Effiency'] = units.Unit_Efficiency_Percent.str.replace('%', '')\
                             .astype(float) / 100
    units = units.groupby('GEO_Assigned_Identification_Number')\
                 .agg({'DateIn': [min, max], 'Effiency': 'mean'})

    _ = geo.projectID.map(units.DateIn['min'])
    geo['DateIn'] = (geo.DateIn.str[:4].apply(pd.to_numeric, errors='coerce')
                     .where(lambda x: x > 1900).fillna(_))

    _ = geo.projectID.map(units.DateIn['max'])
    geo['DateRetrofit'] = geo.DateRetrofit.astype(float).fillna(_)

    _ = units.Effiency['mean']
    geo['Effiency'] = geo.projectID.map(_)
    
    return (geo.assign(projectID=lambda s: 'GEO' + s.projectID.astype(str))
            .query("Country in @countries")
            .replace({col: {'Gas': 'Natural Gas'} for col in
                      {'Fueltype', 'FuelClassification1',
                       'FuelClassification2'}})
            .pipe(gather_fueltype_info, search_col=['FuelClassification1'])
            .pipe(gather_technology_info, search_col=['FuelClassification1'],
                  config=CONFIG)
            .pipe(gather_set_info)
            .pipe(set_column_name, 'GEO')
            .pipe(config_filter, name='GEO', config=CONFIG)
            .pipe(clean_powerplantname)
            .pipe(clean_technology, generalize_hydros=True)
            .pipe(scale_to_net_capacities,
                  (not CONFIG['GEO']['net_capacity']))
            .pipe(config_filter, name='GEO', config=CONFIG)
            .pipe(correct_manually, 'GEO', config=CONFIG)
            )

