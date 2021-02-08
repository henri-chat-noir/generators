import pandas as pd

import globals as glob

def load_search_info_df(file_spec):

    nas_ex_nullstring =  ['#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', '<NA>', 'N/A', 'NA', 'NULL', 'NaN', 'n/a', 'nan', 'null']
    na_val_dict = {'place_name':nas_ex_nullstring, 'formatted_address':nas_ex_nullstring}
    df = pd.read_csv(file_spec, index_col='projectID', na_values=nas_ex_nullstring, keep_default_na=False)

    df['place_id'].where(df['place_id']=="", None)
        
    test = df.place_id

    return df

def parse_geometry(in_df):

    out_df = in_df.copy()
    for projectID, search_info in in_df.iterrows():

        geom_dict = eval(search_info.geometry)

        if geom_dict:
            lat = geom_dict['location']['lat']
            lon = geom_dict['location']['lng']
        else:
            lat = None
            lon = None

        out_df.at[projectID, 'gm_lat'] = lat
        out_df.at[projectID, 'gm_lon'] = lon

    out_df = out_df.drop('geometry', axis=1)

    return out_df

search_ref_spec = glob.ref_data("project_search_info.csv")
search_ref_df = load_search_info_df(search_ref_spec)

new_df = parse_geometry(search_ref_df)

new_df.to_csv(search_ref_spec)