"""Generate land-sea masks for all grids and bases."""
from itertools import product
from sheerwater_benchmarking.reanalysis import era5_agg
from sheerwater_benchmarking.reanalysis.era5 import era5_rolled


vars = ["tmp2m", "precip"]
grids = ["global0_25", "global1_5"]
aggs = [1, 7, 14]
masks = ["lsm"]

start_time = "1979-01-01"
end_time = "2025-01-01"

UPDATE_ROLLED = False
UPDATE_AGG = False

# for var, agg, grid in product(vars, aggs, africa_grids):
for var, agg, grid in product(vars, aggs, grids):
    # Go back and update the earlier parts of the pipeline
    if UPDATE_ROLLED:
        # Update the rolled data for global grids
        ds = era5_rolled(start_time, end_time, variable=var, grid=grid, agg=agg,
                         recompute=True, remote=True, force_overwrite=True)

        for mask in masks:
            if UPDATE_AGG:
                ds = era5_agg(start_time, end_time, variable=var, grid=grid, agg=agg, mask=mask,
                              recompute=True, remote=True, force_overwrite=True)
