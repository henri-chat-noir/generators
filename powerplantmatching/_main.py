import collection
import staging

# df = pd.read_csv(outfn_reduced, index_col=0)
# df = df.pipe(projectID_to_dict)

dfs = staging.load_dataframes()
dfs = staging.tidy_dataframes(dfs)

plant_df = collection.build_plant_df_alldata(dfs)
plant_df = collection.build_plant_df_reduced(plant_df)
# plant_df = refine_plants . . .