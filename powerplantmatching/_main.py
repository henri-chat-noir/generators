import collection
import load_dfs as load


# df = pd.read_csv(outfn_reduced, index_col=0)
# df = df.pipe(projectID_to_dict)

dfs = load.load_dataframes()

plant_df = collection.build_plant_df_alldata(dfs)
plant_df = collection.build_plant_df_reduced(plant_df)
# plant_df = refine_plants . . .