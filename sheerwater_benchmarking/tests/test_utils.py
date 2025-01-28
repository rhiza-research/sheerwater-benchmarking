"""Test the utility functions in the utils module."""
import numpy as np

from sheerwater_benchmarking.utils import get_grid
from sheerwater_benchmarking.utils import base180_to_base360, base360_to_base180

def test_get_grid():
    """Test the get_grid function."""
    grids = ["global1_5", "global0_25"]
    for grid in grids:
        lons, lats, size = get_grid(grid)
        diffs_lon = np.diff(lons)
        diffs_lat = np.diff(lats)
        assert (diffs_lon == size).all()
        assert (diffs_lat == size).all()


def test_lon_convert():
    """Test the get_grid function."""
    # On the boundary of the wrap point in the 360 base
    hard_wrap_180 = np.arange(-10, 10, 1.0)
    # On the boundary of the wrap point in the 180 base
    hard_wrap_360 = np.arange(170, 190, 1.0)

    # Convert bases
    out_360 = base180_to_base360(hard_wrap_180)
    out_180 = base360_to_base180(hard_wrap_360)

    # Check that the output contains a discontinuity
    assert (sorted(out_360) != out_360).all()
    assert (sorted(out_180) != out_180).all()

    # Convert back
    rev_180 = base360_to_base180(out_360)
    rev_360 = base180_to_base360(out_180)
    assert (rev_180 == hard_wrap_180).all()
    assert (rev_360 == hard_wrap_360).all()

    # Convert a single point
    assert base180_to_base360(-179.0) == 181.0
    assert base360_to_base180(0.0) == 0.0
    assert base360_to_base180(359.0) == -1.0
