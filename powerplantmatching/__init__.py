# Copyright 2016-2020 Fabian Hofmann (FIAS), Jonas Hoersch (KIT, IAI) and
# Fabian Gotzens (FZJ, IEK-STE)

"""
powerplantmatching

A set of tools for cleaning, standardising and combining multiple power plant databases.

The rough hierarchy of this package is:
   - collection
        --
        --

        -- core

utils, heuristics, cleaning, matching, data

"""

__version__ = "0.4.7"
__author__ = "Fabian Hofmann, Jonas Hoersch, Fabian Gotzens"
__copyright__ = "Copyright 2017-2020 Frankfurt Institute for Advanced Studies"

# from powerplantmatching.collection import matched_data as powerplants
from powerplantmatching import cleaning, collection, core, data, heuristics, matching, utils

from powerplantmatching.core import package_config # Dictionary of folder locations
from powerplantmatching.core import _get_config # Function that loads config file into dictionary

# from powerplantmatching import plot
# from .accessor import PowerPlantAccessor