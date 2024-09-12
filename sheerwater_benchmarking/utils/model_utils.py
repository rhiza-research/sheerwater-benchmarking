"""Model utility functions for all parts of the evaluation."""

import salientsdk as sk

from .general_utils import get_grid
from .secrets import salient_auth


@salient_auth
def get_salient_loc(grid):
    """Get and upload the location object for the Salient API."""
    if grid != "africa0_25":
        raise NotImplementedError("Only the African 0.25 grid is supported.")

    # Upload location shapefile to Salient backend
    lons, lats, _ = get_grid(grid)
    lons = [float(x) for x in lons]
    lats = [float(x) for x in lats]
    coords = [(lons[0], lats[0]), (lons[1], lats[0]), (lons[1], lats[1]), (lons[0], lats[1])]
    loc = sk.Location(shapefile=sk.upload_shapefile(
        coords=coords,
        geoname="all_africa",  # the full African continent
        force=True))

    return loc
