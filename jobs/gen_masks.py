"""Generate land-sea masks for all grids and bases."""
from itertools import product
from sheerwater_benchmarking.masks import land_sea_mask


grids = ["global0_25", "global1_5"]

for grid in product(grids):
    land_sea_mask_ds = land_sea_mask(grid=grid, recompute=True, force_overwrite=True)
