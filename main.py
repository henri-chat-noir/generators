from powerplantmatching.core import _get_config
import powerplantmatching.collection as ppm_collect

config = _get_config()

"""
**collection_kwargs : kwargs
            Arguments passed to powerplantmatching.collection.Collection.
            Typical arguments are update, use_saved_aggregation,
            use_saved_matches.
"""

collected_df = ppm_collect.collect_datasets(config, rebuild_collection=True)

# plant_df = ppm_collect.build_plants(collected_df, config)
