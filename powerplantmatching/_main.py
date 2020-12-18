import build_plant_tables as plants
import stage

# df = pd.read_csv(outfn_reduced, index_col=0)
# df = df.pipe(projectID_to_dict)

dfs = stage.load_dataframes()
dfs = stage.clean_dataframes(dfs)
dfs = stage.group_units(dfs)

plant_df = plants.build_plant_df_alldata(dfs)
plant_df = plants.build_plant_df_reduced(plant_df)
# plant_df = refine_plants . . .