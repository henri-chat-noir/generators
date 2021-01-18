#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 25 08:48:04 2020

@author: fabian
"""

import powerplantmatching as pm

def test_OPSD():
    pm.data.OPSD()


def test_powerplants():
    pm.powerplants(from_url=True)

def _report_missing_ids(eval_df, ref_ids_set, save=False, id_col='projectID'):

    # identified_missing_ids_dict['ENTSOE'] = ['50WP00000000707U']
    # identified_missing_ids_dict['JRC'] = ['H70', 'H199', 'H223', 'H573', 'H1495', 'H1554', 'H2499', 'H2501', 'H37', 'H2877', 'H2565', 'H3189']

    eval_ids = set(eval_df.index)
    missing_ids_list = []

    ds_name = eval_df.columns.name
    for target_id in all_ids_list:
        if target_id not in eval_ids and target_id not in NOW_GONE_IDS:
            missing_ids_list.append(target_id)

    # Not implemented yet
    if save:
        # file_spec = glob.set_path(f"{ds_name}_debug-{tag}.csv", sub_debug)
        # df.to_csv(file_spec, header=True)          
        file_spec = glob.set_path(f"drop_list-{tag}.csv", SUB_DEBUG)
        out_writer = csv.writer(open("hello.csv",'w') )
        out_writer.writerows(drop_list)

    return missing_ids_list

def _report_new_ids(eval_df, ref_ids_set, save=False, id_col='projectID'):

    identified_new_ids_dict = {}
    for ds_name in ALL_DATASETS:
        identified_new_ids_dict[ds_name] = []

    identified_new_ids_dict['ENTSOE'] = []
    identified_new_ids_dict['JRC'] = []
    
    eval_ids = set(eval_df.index)
    new_ids_list = []

    ds_name = eval_df.columns.name
    for eval_id in eval_ids:
        if eval_id not in all_ids_list and eval_id not in identified_new_ids_dict[ds_name]:
            new_ids_list.append(eval_id)

    # Not implemented yet

    return new_ids_list