def fetch_entsoe():

    # Save files off to _data_in

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

    if CONFIG['entsoe_token'] is not None:
        entsoe_token = CONFIG['entsoe_token']
        df = parse_if_not_stored('ENTSOE', update, CONFIG, parse_entsoe)

    else:
        if update:
            logger.info('No entsoe_token in config.yaml given, '
                        'falling back to stored version.')
        df = parse_if_not_stored('ENTSOE', update, CONFIG)

    if raw:
        return df

    return

def old_scrap():

    if from_url:
        fn = _data_out('matched_data_red.csv')
        url = config['matched_data_url']
        logger.info(f'Retrieving data from {url}')
        df = (pd.read_csv(url, index_col=0)
                .pipe(projectID_to_dict)
                .pipe(set_column_name, 'Matched Data'))
        logger.info(f'Store data at {fn}')
        df.to_csv(fn)
        return df

    return
