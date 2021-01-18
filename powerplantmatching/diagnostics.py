import os
import csv
from datetime import datetime

# import json
import ast
from itertools import combinations
# import numpy as np
import pandas as pd

import file_handling as fh
from utils import get_data
from _globals import ALL_DATASETS, SUB_TAG, NOW_GONE_IDS, MATCH_COLS, SUB_DIAG, SUB_CLEAN, SUB_DEBUG
import _globals as glob
                     
import duke

project_ids_ref_spec = glob.set_path("project_IDs_ref.csv")
PROJECT_IDS_REF_DF = pd.read_csv(project_ids_ref_spec, index_col='projectID')

plants_ref_spec = glob.set_path("Master_Plants_List_REVISED.csv")
PLANTS_REF_DF = pd.read_csv(plants_ref_spec, index_col=None)

intraset_group_gt_spec = glob.package_data("intraset_groups_GT.csv")
INTRASET_GROUPS_GTDF = pd.read_csv(intraset_group_gt_spec, index_col='set_key')


# GENERAL DIAGNOSTIC UTILITIES - NOT STAGE SPECIFIC
# =================================================

def _save_debug(df, tag):

    ds_name = df.columns.name
    file_spec = glob.set_path(f"{ds_name}_debug-{tag}.csv", SUB_DEBUG)

    df.to_csv(file_spec, header=True)          

    return

def _build_plant_id_groups(df, ids_col='projectID'):

    """
    Process 'projectID' column in target df (reduced) plants list:

    {'JRC': ['H71'], 'OPSD': ['OEU5462', 'OEU5463', 'OEU5464'], 'ENTSOE': ['46WPU0000000032W'], 'GEO': ['GEO42918'], 'CARMA': ['CARMA28173']}
    
    . . . can be eva'ld to a 'row_dict' with an intraset listing of ids for each plant.

    'Flattens' row_dict into simple set of all ids on each row, then appended into full list (of sets)
    """

    id_groups_list = []
    ids_data = df[ids_col]
    for row_dict_str in ids_data:
        row_dict = eval(row_dict_str)
        row_id_set = set()
        for ds_name, id_list in row_dict.items():
            id_set = set(id_list)
            row_id_set.update(id_list)

        id_groups_list.append(row_id_set)
    
    return id_groups_list


# CLEANING STAGE
# ==============

def build_ids_dict(df, ids_col='projectID'):

    """
    Builds a simple dictionary of all projectIDs in df
    As a list of IDs keyed for each dataset
    """
    
    ids_data = df[ids_col]
    ids_dict = {}
    for ds_name in ALL_DATASETS:
        ids_dict[ds_name] = []
   
    for row_dict_str in ids_data:
        row_dict = eval(row_dict_str)
        for ds_name, id_list in row_dict.items():
            ids_dict[ds_name] += id_list
    
    # Should probably check to ensure that a given projectID is NOT duplicated in list

    return ids_dict

def build_ids_list(ids_dict, target_datasets=ALL_DATASETS):
   
    """
    Builds single list of all projectIDs in ids_dict
    for all indicated datasets (passed as list)
    """
    target_ids_list = []
    for ds_name in target_datasets:
        id_list = ids_dict[ds_name]
        target_ids_list += id_list

    return target_ids_list

def compare_cleaned_ids(eval_df, report_missing=True, report_new=True):

    ds_name = eval_df.columns.name
    ref_ids_set = set(PROJECT_IDS_REF_DF[PROJECT_IDS_REF_DF.DS_Name == ds_name].index)
    eval_ids_set = set(eval_df.index)

    missing_ids_set = ref_ids_set - eval_ids_set
    matched_ids_set = ref_ids_set.intersection(eval_ids_set)
    new_ids_set = eval_ids_set - ref_ids_set

    all_ids_set = matched_ids_set.union(missing_ids_set, new_ids_set)

    all_ids_list = sorted( list(all_ids_set) )
    all_ids_df = pd.DataFrame(all_ids_list, columns=['projectID'])
    all_ids_df['MatchCode'] = ""
    all_ids_df['Dataset'] = ds_name

    all_ids_df.MatchCode[ all_ids_df.projectID.isin(missing_ids_set) ] = "MISSING"
    all_ids_df.MatchCode[ all_ids_df.projectID.isin(matched_ids_set) ] = "MATCHED"
    all_ids_df.MatchCode[ all_ids_df.projectID.isin(new_ids_set) ] = "NEW"

    all_ids_spec = glob.set_diag_path("all_project_ids.csv", SUB_CLEAN)
    all_ids_df.to_csv(all_ids_spec, index=False, header=True, mode='a+', encoding='utf-8-sig')
    
    if report_missing:
        if missing_ids_set:
            print(f"Missing IDs -- end of cleaning {ds_name}:")
            for id in sorted( list(missing_ids_set) ):
                print(id)
        else:
            print(f"Missing IDs -- end of cleaning {ds_name}: NONE")

    if report_new:
        if new_ids_set:
            print(f"New IDs -- end of cleaning {ds_name}:")
            for id in sorted( list(new_ids_set) ):
                print(id)
        else:
            print(f"New IDs -- end of cleaning {ds_name}: NONE")

    return missing_ids_set, matched_ids_set, new_ids_set


# CLIQUE IDENTIFICATION - INTRASET
# ================================

def build_label_string(id_set, source_df, col_name='OrigName'):

    labels_list = []
    for id in id_set:
        try:
            label = source_df.loc[id, col_name]
            labels_list.append(label)
        except:
            labels_list.append('MISSING')
            stop = True

    label_string = " | ".join(labels_list)
    label_string = f"[{label_string}]"

    return label_string

def load_clean_dfs(ds_name_list=ALL_DATASETS):

    clean_dfs = []
    for ds_name in ds_name_list:
        fn = ds_name + "_clean.csv"
        file_spec = glob.set_path(fn, SUB_CLEAN)
        df = pd.read_csv(file_spec, index_col='projectID')
        clean_dfs.append(df)

    return clean_dfs

def extract_intraset_groups_from_plants(plant_df, ids_col='projectID', display=False, default_truth_status="", drop_gone=True, write_file_tag=None):

    """
    Somewhat of a one-off processing function on a 'final' plants list.
    Processes projectID column, which is a row_dict_str format, e.g.
    
    {'JRC': ['H144'], 'OPSD': ['OEU5262', 'OEU5264'], 'ENTSOE': ['48WSTN00000FOYEQ'], 'GPD': ['GBR2000657', 'GBR0000403'], 'GEO': ['GEO3219'], 'CARMA': ['CARMA13925', 'CARMA13926']}

    which can be eval'd to a Python dict.

    Function evaluates any lists > 1 on the row, which indicates that a specific dataset has contributed more than one
    row of data for the plant in question -- indicating an intraset grouping.

    Organizes data into a dictionary, keyed for each dataset, with list of sets (of projectIDs)
    """

    ids_data = plant_df[ids_col]
    intraset_groups_dict = {}

    clean_dfs = load_clean_dfs()
    
    for row_dict_str in ids_data:
        row_dict = eval(row_dict_str)
        for ds_name, id_list in row_dict.items():

            if drop_gone:
                revised_id_list = [id for id in id_list if id not in NOW_GONE_IDS]
            else:
                revised_id_list = id_list.copy()
            
            if len(revised_id_list) > 1:
                revised_id_list.sort()
                id_set = set(revised_id_list)
                set_key = " | ".join(revised_id_list)
                intraset_groups_dict[set_key] = {}

                id_set = set(revised_id_list)
                intraset_groups_dict[set_key]['id_set'] = id_set
                intraset_groups_dict[set_key]['ds_name'] = ds_name
                intraset_groups_dict[set_key]['truth_status'] = default_truth_status

                now_datetime = datetime.now()
                now_string = now_datetime.strftime("%Y-%m-%d %H:%M:%S")
                intraset_groups_dict[set_key]['mod_time'] = now_string

    if write_file_tag is not None:
        fn = f"intraset_groups_{write_file_tag}.csv"
        # file_spec = glob.set_diag_path(fn, os.path.join(SUB_DIAG, "intraset") )
        # fh.save_flat_dict_of_iterables_to_csv(intraset_groups_dict, file_spec, header_labels=['dataset', 'id_grouping'], item_type='set', fmode='w')

    if display:
        for ds_name, sets_list in intraset_groups_dict.items():
            print(f"\nds_name: {ds_name}")
            print("====================")
            print(sets_list)

    return intraset_groups_dict

def _build_intraset_groups_list(tagged_df, group_col='grouped'):

    """
    For indicated dataset, builds a list of sets.
    Where each set reflects groupings based on Duke-based "grouped' column in saved 'tagged' csv
    """

    intraset_list = []
    # index_ser = pd.Series(tagged_df.index.values)
    group_tagged = tagged_df.groupby( tagged_df[group_col] )
    for group_name, group_data in group_tagged:
        group_ids = group_data.index.values
        id_set = frozenset(group_ids) # Frozen in order for list to be hashable into a master set for cross-checking
        if len(id_set) > 1:
            intraset_list.append(id_set)

    return intraset_list

def build_intraset_issues_dict(eval_df):

    def find_associated_set(target_set, unaccounted_sets):
        for target_id in target_set:
            for eval_set in unaccounted_sets:
                if target_id in eval_set:
                    return eval_set
        return set()

    issues_dict = {}

    ds_name = eval_df.columns.name
    gt_df = INTRASET_GROUPS_GTDF
    gt_intraset_df = gt_df[ (gt_df.dataset == ds_name) & (gt_df.truth_status != "X") & (gt_df.truth_status != "M") ]
    gt_intraset_ser = gt_intraset_df['gt_set']

    # Cyle through all set groups from stored csv and make them frozen sets, so can operate on/with set( frozensets )
    gt_sets = set()
    for idx, sets_string in gt_intraset_ser.iteritems():
        gt_set = eval(sets_string)
        gt_sets.add( frozenset(gt_set) )

    # Generate set (of frozen sets) of dataframe being evaluated for issues (against ground truth)
    eval_sets = set( _build_intraset_groups_list(eval_df) )
        
    matched_groupings = gt_sets.intersection(eval_sets)
    incorrect_groupings = eval_sets - gt_sets
    missed_groupings = gt_sets - eval_sets

    issue_sets = missed_groupings.union(incorrect_groupings)
        
    issues_dict = {}
    for issue_set in issue_sets: # Frozen sets at this point

        if issue_set in incorrect_groupings:
            # Only possible place in ground-truth for eval set elements, if grouped, is within 'missed groupings'
            eval_set = set(issue_set)
            gt_set = set( find_associated_set(issue_set, missed_groupings)) # Might return empty set if no overlap / linkage
        elif issue_set in missed_groupings:
            gt_set = set(issue_set)
            eval_set = set(find_associated_set(issue_set, incorrect_groupings)) # Might return empty set if no overlap / linkage
        else:
            # Should only be two possible variants of set causing an issue          
            stop = True

        superset = eval_set.union(gt_set)

        if len(gt_set) > 0:
            gt_list = sorted( list(gt_set) )
            gt_key = " | ".join(gt_list)
            gt_info = gt_intraset_df.loc[gt_key]
            truth_status = gt_info['truth_status']
        else:
            truth_status = "NONE"

        row_dict = {}
        row_dict['superset'] = superset
        row_dict['gt_set'] = gt_set
        row_dict['eval_set'] = eval_set
        row_dict['dataset'] = ds_name

        # Initialize three possible columns to set dtype, empty sets
        # for col_lab in ['MissingIDs', 'MatchedIDs', 'ExtraIDs']:
            # row_dict[col_lab] = set()
        
        matched_ids = gt_set.intersection(eval_set)
        missing_ids = gt_set - eval_set
        extra_ids = eval_set - gt_set

        row_dict['matched_IDs'] = matched_ids
        row_dict['missing_IDs'] = missing_ids
        row_dict['extra_IDs'] = extra_ids
      
        if matched_ids:
            with_label = build_label_string(matched_ids, eval_df)
        else:
            with_label = None

        comment = ""
        if missing_ids:
            labels = build_label_string(missing_ids, eval_df)
            comment = f"{labels} SHOULD be matched"
            if with_label is not None:
                comment = comment + f" with {with_label}"
        
        if extra_ids:
            labels = build_label_string(extra_ids, eval_df)
            if comment != "":
                comment += "; "
            comment = f"{comment}{labels} should NOT be matched"
            if with_label is not None:
                comment = comment + f" with {with_label}"
        
        if missing_ids and extra_ids:
            truth_str = f"{truth_status}:{truth_status}"
        else:
            truth_str = truth_status
        row_dict['truth_status'] = truth_str

        row_dict['comment'] = comment
        # Change any empty sets to a "NONE" indicator
        for key, val in row_dict.items():
            if len(val) == 0:
                row_dict[key] = "NONE"

        superset_list = sorted( list(superset) )
        row_key = " | ".join(superset_list)
        issues_dict[row_key] = row_dict
            
    return issues_dict

def _get_set_match_code(projectID, superset_info):

    if pd.notnull(superset_info.MatchedIDs) and projectID in superset_info.MatchedIDs:
        match_code = "MATCHED"
    elif pd.notnull(superset_info.MissingIDs) and projectID in superset_info.MissingIDs:
        match_code = 'MISSING'
    elif pd.notnull(superset_info.ExtraIDs) and projectID in superset_info.ExtraIDs:
        match_code = 'EXTRA'
    else:
        stop = True

    return match_code

def list_intraset_group_info(dfs):

    """
    Function that loads the intraset issues file and
    flattens it into one row per projectID, indicating whether that projectID
    is "MATCHED", "MISSING", or "EXTRA" based upon what subset of the
    overall superset it's located in

    """

    load_file_spec = glob.set_path(f"Intraset issues - Superset Info.csv", os.path.join(SUB_DIAG, "intraset"))
    superset_df = pd.read_csv(load_file_spec, index_col='SuperSetKey', low_memory=True)

    issues_df = pd.DataFrame()
    for df in dfs:

        # Cycle through infomration for each superset
        for superset_idx, superset_info in superset_df.iterrows():

            # Check only those rows that match current dataset in loop
            ds_name = superset_info.Dataset
            if ds_name == df.columns.name:
                
                # Retrieve list of projectIDs from stored superset
                super_set = superset_info['SuperSet']
                ids_list = ast.literal_eval(super_set)
                for projectID in ids_list:
                    data_series = get_data(df, projectID) # Retrieve 'plant' data for given projectID
                        
                    data_dict = dict(data_series)

                    match_code = _get_set_match_code(projectID, superset_info)
                    data_dict['MatchStatus'] = match_code
                  
                    data_dict['Dataset'] = ds_name
                    data_dict['GroupKey'] = super_set
                    data_series = pd.Series(data_dict)
                    data_series.name = projectID
                    issues_df = issues_df.append(data_series)

    file_spec = glob.set_path(f"Intraset issues - Project Data.csv", os.path.join(SUB_DIAG, 'intraset') )
    issues_df.index.name = 'projectID'
    if issues_df.empty:
        empty_row = pd.Series(name='Empty')
        issues_df.loc['Empty', 'GroupKey'] = "NO ISSUES FOUND"
        issues_df.to_csv(file_spec)
    else:
        issues_df.to_csv(file_spec, columns=MATCH_COLS)        

    return

def build_intraset_pairs_dict():

    """
    Iterates through saved csv file with 'problem' intraset groups / cliques
    Creates unique (unordered) pair combinations for each set

    """

    load_file_spec = glob.set_path(f"Intraset issues - Superset Info.csv", os.path.join(SUB_DIAG, "intraset") )
    superset_df = pd.read_csv(load_file_spec, index_col='SuperSetKey', low_memory=True)

    intraset_pairs_dict = {}
    for superset_idx, superset_info in superset_df.iterrows():

        # Check only those rows that match current dataset in loop
                
        # Retrieve list of projectIDs from stored superset
        super_set = superset_info['SuperSet']
        ids_list = ast.literal_eval(super_set)

        # Returns list of tuples (0, 1), (0, 2), etc.
        ptr_pairs_list = list( combinations( range( len(ids_list)), 2))
        id_pairs_list = []
        for ptr1, ptr2 in ptr_pairs_list:
            id_pair = (ids_list[ptr1], ids_list[ptr2])
            id_pairs_list.append(id_pair)

        intraset_pairs_dict[superset_idx] = id_pairs_list

    return intraset_pairs_dict

def parse_DebugCompare_stdout(pair_text):

    """
    Takes streamed text from single-pair debug info
    Output in stdout from Duke DebugCompare function
    And outputs a two-part return:
        1.  A data dictionary keyed to each properties (NAME, ESOURCE, TECH, COUNTRY, CAPACITY, GEOPOS)
        2.  Final overall score (as float)

    """
    prop_dtypes = { 'NAME': 'string',
                    'ESOURCE': 'string',
                    'TECH': 'string',
                    'COUNTRY': 'string',
                    'CAPACITY': 'num',
                    'GEOPOS': 'numtup'
                    }

    def parse_prop_data(line_text, prop_name):
        """
        Returns sub-dictionary with each of the four (4) elements of data load
        and properly dtype set, e.g. GEOPOS as a pair of 2tuples
        """

        main_prop_data_dict = {}
        line_text = line_text.replace("'", "")
        front_part, score_prob_part = line_text.split(": ")

        # Process front_part
        val1, val2 = front_part.split(' ~ ')
        if prop_dtypes[prop_name] == 'num':
            val1, val2 = float(val1), float(val2)
        elif prop_dtypes[prop_name] == 'mumtup':
            t1a, t1b = val1.split(",")
            t2a, t2b = val2.split(",")
            val1, val2 = ( float(t1a), float(t2a) ), (float(t2a), float(t2b))
        main_prop_data_dict['val1'] = val1
        main_prop_data_dict['val2'] = val2

        # Process score_prob_part - format common to all properties
        score_prob_part = score_prob_part[:-1] # Drop closing parent from element
        score_text, prob_text = score_prob_part.split(" (prob ")
        main_prop_data_dict['score'] = float(score_text)
        main_prop_data_dict['prob'] = float(prob_text)

        return main_prop_data_dict

    def find_delta(line_text):
        number_text = line_text.replace("Result: ", "")
        tval1, tval2 = number_text.split(" -> ")
        from_val = float(tval1)
        to_val = float(tval2)
        result_delta = to_val - from_val
        return result_delta

    def grab_overall(line_text):
        prefix_len = len("Overall: ")
        value = float(line_text[prefix_len:])
        return value

    prop_data_dict = {}
    pair_data_lines = pair_text.splitlines(keepends=False)
    for line in pair_data_lines:
        print(line)

    if "Reindexing all records" in pair_data_lines[0]:
        pair_data_lines = pair_data_lines[1:]

    for line in pair_data_lines:
        if line.strip() == "": pass

        elif line[:3] == "---":
            prop_name = line[3:]
            prop_data_dict[prop_name] = {}
            
        elif line[:7] == "Result:":
            result_delta = find_delta(line)
            prop_data_dict[prop_name]['delta'] = result_delta

        elif line[:8] == "Overall:": # Final 'overall score' line
            overall_score = grab_overall(line)

        else: # Main data line
            main_prop_data_dict = parse_prop_data(line, prop_name)
            prop_data_dict[prop_name].update(main_prop_data_dict)
                
    return prop_data_dict, overall_score

def build_debug_data_dict(pairs_dict):

    """
    Reads in list of all pair combinations associated with supersets indicated with matching issues
    Runs DebugCompare on each of those via duke.duke_pair_data(id1, id2)
    Calls parse_DebugCompare to parse output strings to construct a (heavily-nested) data dictionary of all diagnostic info


    """

    debug_data_dict = {}
    load_file_spec = glob.set_path(f"Intraset issues - Superset Info.csv", os.path.join(SUB_DIAG, "intraset"))
    superset_df = pd.read_csv(load_file_spec, index_col='SuperSetKey', low_memory=True)

    for superset_key, pairs_list in pairs_dict.items():
        debug_data_dict[superset_key] = {}      

        print(f"Analysis for intraset clique: {superset_key} . . .")
        stdout_list = [] # Entries are stream of text for each pair combination in set
        stdout_strings = ""

        superset_info = superset_df.loc[superset_key]
        for pair_key in pairs_list: # Pairs list based on projectIDs
            id1, id2 = pair_key
            stdout_str = duke.duke_pair_data(id1, id2)
            
            debug_data_dict[superset_key][pair_key] = {} # Set up empty sub-dict for each pair_key combo in superset

            single_pair_debug_dict, overall_score = parse_DebugCompare_stdout(stdout_str)
            debug_data_dict[superset_key][pair_key].update(single_pair_debug_dict)
            debug_data_dict[superset_key][pair_key]['overall_score'] = overall_score

            stat1 = _get_set_match_code(id1, superset_info)
            stat2 = _get_set_match_code(id2, superset_info)
            pair_match_status = (stat1, stat2)
            debug_data_dict[superset_key][pair_key]['MatchStatus'] = pair_match_status

            stdout_list.append(stdout_str)
            stdout_strings += stdout_str
     
        file_tag = superset_key.replace(" | ", "_")
        output_fn = f"stdout - {file_tag}.txt"
        output_spec = glob.set_path(output_fn, os.path.join(SUB_DIAG, "intraset") )
        with open(output_spec, "w") as text_file:
            text_file.write(stdout_strings)

    return debug_data_dict

def save_debug_dict_to_csv(debug_dict, filename=None):

    output_list = []
    prop_names = ['NAME', 'ESOURCE', 'TECH', 'COUNTRY', 'CAPACITY', 'GEOPOS']

    for superset_key, set_data_dict in debug_dict.items():
        
        for pair_tup, pair_data_dict in set_data_dict.items():

            match_status = pair_data_dict['MatchStatus']
            match_set = set(match_status)
            if match_set == {'MATCHED', 'MATCHED'}:
                next

            else:
                for prop_name in prop_names:
                    row_dict = {}
                    row_dict['set_key'] = superset_key
                    row_dict['id_pair'] = pair_tup
                    if match_set == {'MISSING', 'EXTRA'}:
                        mismatch_code = "MISSING+EXTRA"
                    elif 'MISSING' in match_set:
                        mismatch_code = "MISSING"
                    elif 'EXTRA' in match_set:
                        mismatch_code = "EXTRA"
                    else:
                        stop = True
                        
                    row_dict['mismatch_code'] = mismatch_code
                    row_dict['pair_score'] = pair_data_dict['overall_score']
                    row_dict['prop_name'] = prop_name
                
                    if pair_data_dict[prop_name]:
                        for prop_key, val in pair_data_dict[prop_name].items():
                            row_dict[prop_key] = val
                        if row_dict['score'] != 1:
                            output_list.append(row_dict)

    if filename is None:
        filename = "Intraset issues - Detailed Debug.csv"
        file_spec = glob.set_path(filename, os.path.join(SUB_DIAG, 'intraset') )
        
    header_labels = ['set_key', 'id_pair', 'mismatch_code', 'pair_score', 'prop_name', 'val1', 'val2', 'score', 'prob', 'delta']

    with open(file_spec, encoding='utf-8-sig', mode='w') as save_file:
        csv_writer = csv.writer(save_file, delimiter=',', lineterminator='\n', quotechar='"', quoting=csv.QUOTE_ALL)
        if header_labels:
            csv_writer.writerow(header_labels)

        for row_dict in output_list:
            row_data = []
            for col_label in header_labels:
                data_elem = row_dict[col_label]
                row_data.append(data_elem)
            
            csv_writer.writerow(row_data)

    return


# CROSS-DATASET MTACHING
# ===================================

