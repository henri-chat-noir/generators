from datetime import datetime
import pandas as pd

from _globals import NOW_GONE_IDS, SUB_CLEAN, SUB_TAG
import _globals as glob
import file_handling as fh
import diagnostics as diag

"""
More of an iterative, than a linear set of processing stages:
1. Establish initial baseline of sets, e.g. from Fresna.  (Provisional assignment of truth_status code = "P", for 'presumed correct'.)
2. Measure all differentials against (presumed) ground truth.  Are mismatches incorrect in ground truth (GT) or eval sets?
3. Use ground_truth_truth_status_updates.xlsx UI to mark differentials on either or both 'missing' and 'extra' IDs:
    a.  "x".  Incorrect GT positions
    b.  "c".  GT is correct, so need to evaluate what/how to fix eval.  No fundamental change to GT, just upgrade from "P" to "C" truth_status.
              (or ignore as/if correct truth_status on the 'extra' IDs, as this will not result in a set entry on gt list)
    c.  "u".  Unclear, uncertain.  From names only, not obvious, and further data / eval required.

4.  Recycle / update GT list to reflect truth_status markings above
    This can take one or two iterations depending on nature of sets with > 2 ids in them.  So, for instance . . .
    A, A, B would be marked wrong.  But the system might (then still) match A, A on next iteration, but it won't (yet) have been entered
    Onto GT list on the first instance of marking A, A, B as wrong, at least not automatically / necessarily.

5.  Re-run intraset grouping routine against upgraded GT set.
    Evaluate remaining entries (which now should be only "c" and "u" truth_status) based on detailed Duke probability data

6.  Tune parameters in Duke matching, but also consider:
    a.  White- or black-listing specific sets that machine is not going to delineate
    b.  Reviewing where these sets exist on probability spectrum, and evaluate similar scores to see if issues
    c.  Look at the 'character' of these uncertain matches, e.g. short and/or common words remaining, and mark other examples
        for possible concern
    d.  What can we do with uncertain/unclear (U-marked) groupings?

"""

# FULL LIST OF (ORIGINAL) PROJECT IDS
# ref_ids_dict = diag.build_ids_dict(PLANTS_REF_DF)
# fresna_spec = glob.package_data('project_IDs_fresna.csv')
# fh.save_flat_dict_of_iterables_to_csv(ref_ids_dict, fresna_spec, header_labels=['dataset', 'projectID'], item_type='str', fmode='w')

def build_fresna_intraset():

    # INITIAL 'GROUND TRUTH' LISTING OF INTRASET GROUPS (FROM FRESNA)
    ref_sets_dict = diag.extract_intraset_groups_from_plants(diag.PLANTS_REF_DF, drop_gone=False, default_truth_status="P")
    fresna_spec = glob.package_data("intraset_groups_fresna.csv")
    # fh.save_flat_dict_of_iterables_to_csv(ref_sets_dict, fresna_spec, header_labels=['dataset', 'intraset_group'], item_type='str', fmode='w')

    fh.save_nested_dict_to_csv(ref_sets_dict, fresna_spec, main_key_labels=['set_key'], selected_keys=[], fmode='w')

    return

def update_gt_for_missing_ids():

    def add_set_to_gt(new_set, old_set_info):
        
        new_info = pd.Series()
        new_info['dataset'] = old_set_info['dataset']
        new_info['truth_status'] = 'P'
        new_info['gt_set'] = new_set
        new_info['names'] = ""

        new_info['mod_time'] = mod_time_str

        new_id_list = sorted( list(new_set) )
        new_key = " | ".join(new_id_list)
        
        new_intraset_gt_df.loc[new_key] = new_info

        return

    # Current ground truth
    intraset_gt_spec = glob.package_data("intraset_groups_GT.csv")
    intraset_gt_df = pd.read_csv(intraset_gt_spec, index_col='set_key')

    new_intraset_gt_df = intraset_gt_df.copy()
    for set_key, set_info in intraset_gt_df.iterrows():

        gt_set = eval( set_info['gt_set'] )
        updated_set = gt_set.copy()
        for id in gt_set:
            if id == 'H2499':
                stop = True

            if id in NOW_GONE_IDS:
                updated_set.remove(id)

        if updated_set != gt_set:
            mod_datetime = datetime.now()
            mod_time_str = mod_datetime.strftime("%Y-%m-%d %H:%M:%S")
        
            if len(updated_set) > 1:
                add_set_to_gt(updated_set, set_info)

            set_info['truth_status'] = 'M'
            set_info['mod_time'] = mod_time_str
            new_intraset_gt_df.loc[set_key] = set_info

    new_intraset_gt_df.to_csv(intraset_gt_spec)

    return

def update_intraset_ground_truth():

    def update_name_info(set_key, names_set):

        ds_name = superset_info['dataset']
        intraset_gt_df.loc[set_key, 'dataset'] = ds_name
        
        names = diag.build_label_string(names_set, clean_df)
        intraset_gt_df.loc[set_key, 'names'] = names
        
        return

    def update_gt_set(superset_info, truth_status, mod_time_str=None):

        gt_set = eval( superset_info['gt_set'] )
        gt_list = sorted( list(gt_set) )
        gt_key = " | ".join(gt_list)

        intraset_gt_df.loc[gt_key, 'dataset'] = superset_info['dataset']
        intraset_gt_df.loc[gt_key, 'mod_time'] = mod_time_str
        intraset_gt_df.loc[gt_key, 'truth_status'] = truth_status
        
        update_name_info(gt_key, gt_set)

        return

    def add_eval_set(superset_info, mod_time_str=None, existing_gt=False): # Called when eval_set has unclear or correct groupings
        
        eval_set = eval( superset_info['eval_set'] )
        if existing_gt:
            gt_set = eval( superset_info['gt_set'] )
            gt_set = gt_set.union(eval_set)
        else:
            gt_set = eval_set # New ground truth equal to eval_set

        gt_list = sorted( list(gt_set) )
        gt_key = " | ".join(gt_list)

        intraset_gt_df.loc[gt_key, 'gt_set'] = gt_set
        intraset_gt_df.loc[gt_key, 'mod_time'] = mod_time_str

        if extra_truth_status == "X":
            intraset_gt_df.loc[gt_key, 'truth_status'] = "C"
        else:
            intraset_gt_df.loc[gt_key, 'truth_status'] = "U"
        
        update_name_info(gt_key, gt_set) # Adds ds_name and label strings for IDs in set

        return

    # Excel file that has manually-judged status code updates on each superset
    truth_status_fn = "ground_truth_status_updates.xlsx"
    file_spec = glob.set_diag_path(truth_status_fn, SUB_TAG)
    intraset_update_df = pd.read_excel(file_spec, "Superset_Listing", index_col="superset_key")

    # Current ground truth
    intraset_gt_spec = glob.package_data("intraset_groups_GT.csv")
    intraset_gt_df = pd.read_csv(intraset_gt_spec, index_col='set_key')

    file_spec = glob.set_path("ALL_DATA_clean.csv", SUB_CLEAN)
    clean_df = pd.read_csv(file_spec, index_col='projectID')

    for superset_key, superset_info in intraset_update_df.iterrows():
        
        mod_datetime = datetime.now()
        mod_time_str = mod_datetime.strftime("%Y-%m-%d %H:%M:%S")

        truth_status_str = superset_info['truth_status'].upper()
        if superset_info.missing_IDs == "NONE":
            missing_truth_status = None
        else:
            missing_truth_status = truth_status_str[0]

        if superset_info.extra_IDs == "NONE":
            extra_truth_status = None
        else:
            extra_truth_status = truth_status_str[-1]
        
        # Note this is just a info and truth_status update (from 'P') that may exist on current GT list, where effect as follows:
        #   - 'X' is material, as it as it flags out GT entry from being loaded for future checks
        #   - 'C' simply marks entry with a manual 'definitely' correct truth_status
        #   - 'U', 'unclear' or 'uncertain' usefulness is unclear, but allows specific / separate review from Ps and Cs
        if missing_truth_status in {'X', 'C', 'U'}:
            update_gt_set(superset_info, missing_truth_status, mod_time_str)

        # Either a possible grouping ('U'), or a clearly wrong 'X' missed grouping (hence eval_set with correct grouping)
        # Causes a new or modified entry in the GT list reflecting eval_set
        if extra_truth_status in {'U', 'X'}:
            
            # Eval_set might interesect with existing uncertain or clearly correct grouping
            # So we need to work with a union operation on existing GT row in these cases
            # In these cases, create a new row with with complete set (but also x out existing, partial gt_set)
            if missing_truth_status in {'C', 'U'}:
                add_eval_set(superset_info, mod_time_str, existing_gt=True)
                update_gt_set(superset_info, 'X', mod_time_str)

            else: # None or 'X' incorrect, then simply add new row to GT list
                add_eval_set(superset_info, mod_time_str, existing_gt=False)

    intraset_gt_df.to_csv(intraset_gt_spec)

    return