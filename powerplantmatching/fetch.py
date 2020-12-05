

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
