"""Generate land-sea masks for all grids and bases."""
from itertools import product
from sheerwater_benchmarking.baselines import climatology
from sheerwater_benchmarking.utils import get_config


vars = ["tmp2m", "precip"]
grids = ["global0_25", "global1_5", "africa0_25", "africa1_5"]
# grids = ["africa0_25", "africa1_5"]
masks = ["lsm"]

for var, grid, mask in product(vars, grids, masks):
    # Update standard 30-year climatology
    ds = climatology(1991, 2020, variable=var, grid=grid, mask=mask,
                     recompute=True, remote=True, force_overwrite=True,
                     remote_config=get_config('genevieve')
                     )
