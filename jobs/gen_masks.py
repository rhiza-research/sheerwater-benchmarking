"""Generate land-sea masks for all grids and bases."""
from itertools import product
from sheerwater_benchmarking.masks import land_sea_mask


bases = ["base360", "base180"]
grids = ["global0_25", "global1_5", "africa0_25", "africa1_5"]

for base, grid in product(bases, grids):
    land_sea_mask_ds = land_sea_mask(grid=grid, recompute=True, force_overwrite=True)
