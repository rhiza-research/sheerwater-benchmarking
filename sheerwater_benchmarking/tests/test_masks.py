"""Test the masking functions."""
from itertools import product
from sheerwater_benchmarking.masks import land_sea_mask


def test_masks():
    """Test the land sea mask function."""
    bases = ["base360", "base180"]
    grids = ["global0_25", "global1_5", "africa0_25", "africa1_5"]

    for base, grid in product(bases, grids):
        lsm = land_sea_mask(grid=grid)
        assert len(lsm.lat.values) > 0
