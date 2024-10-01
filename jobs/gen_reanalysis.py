"""Generate land-sea masks for all grids and bases."""
from itertools import product
from sheerwater_benchmarking.reanalysis import era5_agg
from sheerwater_benchmarking.reanalysis.era5 import era5_rolled
from sheerwater_benchmarking.utils import get_config


vars = ["tmp2m", "precip"]
grids = ["global0_25", "global1_5", "africa0_25", "africa1_5"]
# grids = ["africa0_25", "africa1_5"]
# grids = ["global0_25", "global1_5"]
# grids = ["global1_5", "africa1_5"]
# grids = ["global0_25", "africa0_25"]
aggs = [1, 4, 17]
masks = ["lsm"]

start_time = "1979-01-01"
end_time = "2024-09-01"

UPDATE_ROLLED = False

# for var, agg, grid in product(vars, aggs, africa_grids):
for var, agg, grid in product(vars, aggs, grids):
    # Go back and update the earlier parts of the pipeline
    if UPDATE_ROLLED and 'global' in grid:
        # Update the rolled data for global grids
        ds = era5_rolled(start_time, end_time, variable=var, grid=grid, agg=agg,
                         recompute=True, remote=True, force_overwrite=True,
                         remote_config=get_config('genevieve')
                         )
    for mask in masks:
        ds = era5_agg(start_time, end_time, variable=var, grid=grid, agg=agg, mask=mask,
                      recompute=True, remote=True, force_overwrite=True,
                      remote_config=get_config('genevieve')
                      )
