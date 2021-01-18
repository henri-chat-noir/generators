import pandas as pd

# from _globals import PLANTS_REF_DF
import ground_truth as gt
import diagnostics as diag
import build_plant_tables as plants
import stage
import duke

"""

fresna_list = diag.build_id_groups()

# plant_fn = "plants_list_CARMA_ENTSOE_ESE_GEO_GPD_JRC_OPSD_DE_OPSD_EU.csv"
# plant_fn = "plants_list_CARMA_ENTSOE_GEO_JRC_OPSD_DE_OPSD_EU.csv"
plant_fn = "plants_list_CARMA_ENTSOE_GEO_GPD_JRC_OPSD_DE_OPSD_EU.csv"

plant_spec = _set_path(plant_fn, SUB_OUT)
plants_df = pd.read_csv(plant_spec, index_col=0)
current_list = diag.build_id_groups(plants_df)

missing_list = diag.find_sets_A_not_in_B(list_A=fresna_list, list_B=current_list)
print("Missing ID set groups:")
for id_set in missing_list:
    print(id_set)

"""

# diag.restructure_master() # One-off run to bifurcate OPSD into OPSD_EU and OPSD_DE encoding tags (based on ProjectIDs indicated)

# id1 = '16WFRADES------T'
# id2 = '16WFRADES2-----8'
# duke.duke_pair_data(id1, id2, country=None, test_sub=None)


# RESET / UPDATE GROUND TRUTH
# ===========================
# gt.update_intraset_ground_truth()
# update_gt_for_missing_ids()

dfs = stage.load_dataframes()
dfs = stage.clean_dataframes(dfs, run_diag=True, save_missing=True)
dfs = stage.tag_cliques(in_dfs=dfs, run_diag=True)

# dfs = stage.aggregate_units(dfs)

# plant_df = plants.build_plant_df_alldata(dfs)
# plant_df = plants.build_plant_df_reduced(plant_df)

# plant_df = refine_plants . . .

# dups = diag.find_dups(final_ids)
# print(f"Duplicate IDs: {dups}")

# print (final_ids)

# df = pd.read_csv(outfn_reduced, index_col=0)
# df = df.pipe(projectID_to_dict)