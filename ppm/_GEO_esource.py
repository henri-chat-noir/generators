import pandas as pd

import cleaning_functions as clean
from _globals import _set_path, CONFIG, SUB_LAND, SUB_TEST

import special
import diagnostics as diag


diag.extract_intraset_groups()

ds_name = "GEO"
ds_config = CONFIG[ds_name]
ds_spec = _set_path(ds_config['fn'], SUB_LAND)
in_df = pd.read_csv(ds_spec, index_col='GEO_Assigned_Identification_Number', low_memory=False, encoding='utf-8')

esource_col = 'Type'
primary_col = 'Type_of_Fuel_rng1_Primary'
secondary_col = 'Type_of_Fuel_rng2_Secondary'

out_df = clean.parse_fuel_cols(in_df, esource_col, primary_col, secondary_col)
out_cols = [esource_col, primary_col, secondary_col] + ['PrimaryFuel', 'FuelSpecies', 'FuelGenus', 'esource_id', 'SecondaryFuels']

file_spec = _set_path("GEOfuels.csv", SUB_TEST)
out_df.to_csv(file_spec, header=True, columns=out_cols, encoding='utf-8')      

fuel_labels = special.extract_fuel_labels(out_df)
for fuel in fuel_labels:
    print(fuel)