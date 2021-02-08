import random
import pandas as pd
import logging

import ppm.globals as glob
from ppm.globals import CONFIG, DATASET_LABELS, SUB_LAND, SUB_CLEAN, SUB_GOOGLE, SUB_TAG, SUB_GROUP, SUB_FILTER
from ppm.globals import TAG_CLEAN, TAG_FILTER, TAG_GOOGLE

import file_handling as fh
import diagnostics as diag

from utils import parmap
import augment
import google_api as google
import cleaning_functions as clean

import data
logger = logging.getLogger(__name__)

def _load_previous_stage(prev_stage_sub, prev_stage_tag, index_col=None):

    # dtype Type name or dict of column -> type, optional
    # Data type for data or columns. E.g. {‘a’: np.float64, ‘b’: np.int32, ‘c’: ‘Int64’}
    # Use str or object together with suitable na_values settings to preserve and not interpret dtype.
    # If converters are specified, they will be applied INSTEAD of dtype conversion.

    dtype_dict = {'KeywordName': str}

    dfs = []
    for ds_name in DATASET_LABELS:
        load_fn = f"{ds_name}_{prev_stage_tag}.csv"
        file_spec = glob.set_path(load_fn, prev_stage_sub)

        nas_ex_nullstring =  ['#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', '<NA>', 'N/A', 'NA', 'NULL', 'NaN', 'n/a', 'nan', 'null']
        # na_val_dict = {'KeywordName': nas_ex_nullstring}

        df = pd.read_csv(file_spec, index_col='projectID', na_values=nas_ex_nullstring, keep_default_na=False)
        # df['KeywordName'].where(df['KeywordName']==math.nan, "")
        df.columns.name = ds_name
        dfs.append(df)

        for idx, word in df['KeywordName'].iteritems():
            if isinstance(word, float):
                stop = True
            
    return dfs

def load_dataframes(random_pick=None):

    def load_df(ds_name):
        
        ds_config = CONFIG[ds_name]
        root_fn = ds_config['fn']
        if random_pick == 0:
            ds_spec = glob.set_path("RANDOM " + root_fn, SUB_LAND)
        else:
            ds_spec = glob.set_path(root_fn, SUB_LAND)

        if ds_name in {'CARMA', 'GEO'}:
            low_memory = False
        else:
            low_memory = True

        if ds_name == 'GPD':
            # index_col="gppd_idnr"
            index_col=0

        else:
            index_col=None

        # df = pd.read_csv(ds_spec, index_col=index_col, low_memory=low_memory, encoding='utf-8')
        df = pd.read_csv(ds_spec, low_memory=low_memory, encoding='utf-8')

        if random_pick: # Excludes 0 AND None
            max_number = df.shape[0]
            indices = set( random.sample( range(0, max_number), random_pick))
            df = df.loc[indices]
            ds_spec = glob.set_path("RANDOM " + root_fn, SUB_LAND)
            df.to_csv(ds_spec)

        df.columns.name = ds_name        

        return df

    dfs = []
    print("\nLOADING\n" + 40*"=")
    for ds_name in DATASET_LABELS:
        print(f"Loading csv to form dataframe {ds_name}")
        df = load_df(ds_name)
        dfs.append(df)

    return dfs

def clean_dataframes(in_dfs, drop_new=True, run_diag=False, save_missing=False, save_all_df=False):

    out_dfs = []
    project_id_dict = {}
    all_ids_df = pd.DataFrame()

    print("\n\nCLEANING\n" + 40*"=")
    for df in in_dfs:
        ds_name = df.columns.name
        ds_config = CONFIG[ds_name]
        
        print(f"\nCleaning dataset {ds_name} . . .")

        clean_df_func = getattr(data, ds_name)
        df = clean_df_func(df, **ds_config.get('read_kwargs', {}))
        
        if run_diag:
            # Returns simple sets of IDs in each group
            (missing_ids, matched_ids, new_ids) = diag.compare_cleaned_ids(df, report_missing=True, report_new=False)
            # if drop_new: df = df.drop(new_ids)

        if save_missing and len(missing_ids) > 0:
            fn = f"Missing_IDs - {ds_name}.txt"
            file_spec = glob.set_diag_path(fn, sub_dir=SUB_CLEAN)
            fh.save_iterable_to_text(missing_ids, file_spec)

        df = clean.add_geoposition_for_duke(df)

        df['DS_Name'] = ds_name
        file_spec = glob.set_path(f"{ds_name}_{TAG_CLEAN}.csv", SUB_CLEAN)
        df.to_csv(file_spec, header=True, encoding='utf-8')          

        out_dfs.append(df)
        
    print("")
    
    if save_all_df:
        all_clean_df = pd.concat(out_dfs)    
        all_clean_spec = glob.set_path(f"ALL_DATA_clean.csv", SUB_CLEAN)
        all_clean_df.to_csv(all_clean_spec, header=True, encoding='utf-8-sig')

    return out_dfs

def filter_dataframes(in_dfs, id_filter=True):

    if in_dfs is None:
        in_dfs = _load_previous_stage(SUB_CLEAN, TAG_CLEAN, index_col='projectID')        

    out_dfs = []
    for df in in_dfs:
        if id_filter:
            file_spec = glob.set_diag_path("filter_ids.txt", sub_dir=SUB_FILTER)
            filter_list = fh.load_text_to_list(file_spec)
            df = df.filter(filter_list, axis='index')

        ds_name = df.columns.name
        file_spec = glob.set_path(f"{ds_name}_{TAG_FILTER}.csv", SUB_FILTER)
        df.to_csv(file_spec, header=True, encoding='utf-8')          

    out_dfs.append(df)

    return out_dfs

def google_search(in_dfs, force_refresh=False, run_diag=False):

    out_dfs = []
    if in_dfs is None:
        in_dfs = _load_previous_stage(SUB_FILTER, TAG_FILTER, index_col='projectID')        

    print("\n\nGOOGLE_API\n" + 40*"=")
    for df in in_dfs:
        ds_name = df.columns.name
        ds_config = CONFIG[ds_name]
        
        print(f"\nAdding Google API place_id info for dataset {ds_name} . . .")

        out_df = google.get_place_ids(df, max_api_limit=200, force_refresh=force_refresh)
        # out_df = pd.concat([df, place_id_info_df], join='outer', axis=1)

        print(f"\nAdding place details info for dataset {ds_name} . . .")
        # place_info_df = google.get_place_details(out_df)
        # out_df = pd.concat([df, place_info_df], join='outer', axis=1)

        file_spec = glob.set_path(f"{ds_name}_{TAG_GOOGLE}.csv", SUB_GOOGLE)
        out_df.to_csv(file_spec, header=True, encoding='utf-8')
      
        out_dfs.append(df)

    return out_dfs

def tag_cliques(in_dfs=None, run_diag=False):

    if in_dfs is None:
        in_dfs = _load_previous_stage(SUB_CLEAN, TAG_CLEAN, index_col='projectID')        

    out_dfs = []
    issues_dict = {}
    for df in in_dfs:
        ds_name = df.columns.name
        ds_config = CONFIG[ds_name]
        df = augment.tag_cliques_for_single_ds(df)

        if run_diag:
            issues_dict.update(diag.build_intraset_issues_dict(df))

        file_spec = glob.set_path(f"{ds_name}_tagged.csv", SUB_TAG)
        df.to_csv(file_spec, header=True, encoding='utf-8-sig')          
        out_dfs.append(df)

    if run_diag:
        issues_df = pd.DataFrame(issues_dict).T
        issues_df.index.name = 'superset_key'

        file_spec = glob.set_diag_path(f"Intraset issues - Superset Info.csv", SUB_TAG)
        issues_df.to_csv(file_spec, header=True, encoding='utf-8-sig')                  

        # diag.list_intraset_group_info(dfs)
        # pairs_dict = diag.build_intraset_pairs_dict()
        # debug_dict = diag.build_debug_data_dict(pairs_dict) # Runs Duke DebugCompare pairs-analysis utility
        # diag.save_debug_dict_to_csv(debug_dict)

    return out_dfs

def aggregate_units(in_dfs):

    out_dfs = []
    for df in in_dfs:
        ds_name = df.columns.name
        ds_config = CONFIG[ds_name]
        if ds_config.get('aggregated_units', False):
            df = df.assign(projectID=df.projectID.map(lambda x: [x]))
        else:
            df = augment.aggregate_units_for_single_ds(df, use_saved_dukemap=True)

        file_spec = glob.set_path(f"{ds_name}_grouped.csv", SUB_GROUP)
        df.to_csv(file_spec, header=True, encoding='utf-8')          
        out_dfs.append(df)

    return out_dfs