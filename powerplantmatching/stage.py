import pandas as pd
import logging

import _globals as glob
import file_handling as fh
import diagnostics as diag

from _globals import CONFIG, DATASET_LABELS, SUB_LAND, SUB_CLEAN, SUB_TAG, SUB_GROUP
from _globals import TAG_CLEAN

from utils import parmap
import augment
import cleaning_functions as clean


import data
logger = logging.getLogger(__name__)

def _load_previous_stage(prev_stage_sub, prev_stage_tag, index_col=None):

    dfs = []
    for ds_name in DATASET_LABELS:
        load_fn = f"{ds_name}_{prev_stage_tag}.csv"
        file_spec = glob.set_path(load_fn, prev_stage_sub)
        df = pd.read_csv(file_spec, index_col=index_col)
        df.columns.name = ds_name
        dfs.append(df)

    return dfs

def load_dataframes():

    def load_df(ds_name):
        ds_config = CONFIG[ds_name]
        ds_spec = glob.set_path(ds_config['fn'], SUB_LAND)
        if ds_name in {'CARMA', 'GEO'}:
            low_memory = False
        else:
            low_memory = True

        if ds_name == 'GPD':
            # index_col="gppd_idnr"
            index_col=0

        else:
            index_col=None

        df = pd.read_csv(ds_spec, index_col=index_col, low_memory=low_memory, encoding='utf-8')
        df.columns.name = ds_name        

        return df

    dfs = []
    print("\nLOADING\n" + 40*"=")
    for ds_name in DATASET_LABELS:
        print(f"Loading csv to form dataframe {ds_name}")
        df = load_df(ds_name)
        dfs.append(df)

    return dfs

def clean_dataframes(in_dfs, drop_new=True, run_diag=False, save_missing=False):

    out_dfs = []
    project_id_dict = {}
    all_ids_df = pd.DataFrame()

    print("\n\nCLEANING\n" + 40*"=")
    for df in in_dfs:
        ds_name = df.columns.name
        ds_config = CONFIG[ds_name]
        
        print(f"\nCleaning dataset {ds_name} . . .")

        clean_df_func = getattr(data, ds_name)
        df = clean_df_func(in_df=df, **ds_config.get('read_kwargs', {}))

        if run_diag:
            # Returns simple sets of IDs in each group
            (missing_ids, matched_ids, new_ids) = diag.compare_cleaned_ids(df, report_missing=True, report_new=False)
            if drop_new: df = df.drop(new_ids)

        if save_missing and len(missing_ids) > 0:
            fn = f"Missing_IDs - {ds_name}.txt"
            file_spec = glob.set_diag_path(fn, sub_dir=SUB_CLEAN)
            fh.save_iterable_to_text(missing_ids, file_spec)

        df = clean.add_geoposition_for_duke(df)

        df['DS_Name'] = ds_name
        file_spec = glob.set_path(f"{ds_name}{TAG_CLEAN}.csv", SUB_CLEAN)
        df.to_csv(file_spec, header=True, encoding='utf-8')          

        out_dfs.append(df)
        
    print("")
    
    if run_diag:
        all_clean_df = pd.concat(out_dfs)    
        all_clean_spec = glob.set_path(f"ALL_DATA_clean.csv", SUB_CLEAN)
        all_clean_df.to_csv(all_clean_spec, header=True, encoding='utf-8-sig')

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