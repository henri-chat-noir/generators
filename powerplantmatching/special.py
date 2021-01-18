import pandas as pd

from _globals import MASTER

def restructure_master(in_df=MASTER, ids_col='projectID', save_file=True):

    """
    One-off processing of original PPM data file, which presented
    OPSD_DE and OPSD_EU as concatenated into single OPSD DataFrame
    This routine simply relablels these entries to be consistent with curren format.

    """

    out_df = in_df.copy()
    ids_data = out_df[ids_col]    
    out_dicts_list = []
    for row_dict_str in ids_data:
        current_row_dict = eval(row_dict_str)
        new_row_dict = {}
        for ds_name, id_list in current_row_dict.items():
            if ds_name == "OPSD":
                de_id_list = []
                eu_id_list = []
                for id in id_list:
                    opsd_prefix = id[:3]
                    if opsd_prefix == "BNA":
                        de_id_list.append(id)
                    elif opsd_prefix == "OEU":
                        eu_id_list.append(id)
                    else:
                        print(f"Unidentified OPSD projectID prefix: {opsd_prefix}")
                        stop = True

                if len(de_id_list) > 1:
                    new_row_dict['OPSD_DE'] = de_id_list

                if len(eu_id_list) > 1:
                    new_row_dict['OPSD_EU'] = eu_id_list

            else:
                new_row_dict[ds_name] = id_list

        out_dicts_list.append(new_row_dict)

    out_df[ids_col] = out_dicts_list

    if save_file:
        file_spec = _set_path("Master_Plants_List_REVISED.csv")
        out_df.to_csv(file_spec)
    
    return out_df

def extract_fuel_labels(in_df, save_file=True):

    type_labels = in_df['Type'].unique()
    type_labels = pd.Series(type_labels).dropna()
    type_labels = type_labels.str.lower()
    type_labels = set(type_labels)
    
    primary_labels = in_df['PrimaryFuel'].unique()
    primary_labels = pd.Series(primary_labels).dropna()
    primary_labels = set(primary_labels)
    
    secondary_list = []
    for idx, secondary_str in in_df['SecondaryFuels'].iteritems():
        if secondary_str != "":
            secondary_set = eval(secondary_str)
            secondary_list += list(secondary_set)
    secondary_labels = set(secondary_list)

    fuel_labels = type_labels.union(primary_labels, secondary_labels)
    fuel_labels = sorted( list(fuel_labels) )
    # fuel_species.remove("")

    return fuel_labels