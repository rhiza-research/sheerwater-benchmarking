"""Pulls GraphCast forecasts from the Graphcast bucket."""
import dateparser

import numpy as np
import xarray as xr

from sheerwater_benchmarking.utils import (cacheable, dask_remote,
                                           roll_and_agg,
                                           get_variable,
                                           apply_mask, clip_region,
                                           lon_base_change,
                                           regrid)


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'grid'],
           timeseries='time',
           cache=False)
def graphcast_raw(start_time, end_time, variable, grid="global0_25"):  # noqa ARG001
    """ECMWF function that returns data from Google Weather Bench."""
    if grid != "global0_25":
        raise NotImplementedError("Only ERA5 native 0.25 degree grid is implemented.")

    years = [2019, 2020, 2021, 2022]
    diffs = [2, 2, 1, 1]
    months = ["01", "01", "12", "12"]
    filenames = [f'gs://graphnn-historical-weather-forecasts/operational/forecasts_10d/date_range_{
        year}-12-01_{year+diff}-{month}-22_6_hours.zarr' for diff, year, month in zip(diffs, years, months)]
    filenames

    # Pull the google dataset
    ds = xr.open_mfdataset(filenames, chunks={'latitude': 721, 'longitude': 1440, 'time': 30, 'lead_time': 1})

    var = get_variable(variable, 'graphcast')
    # Select the right variable
    ds = ds[var].to_dataset()
    ds = ds.rename_vars(name_dict={var: variable})

    # Convert local dataset naming and units
    ds = ds.rename({'latitude': 'lat', 'longitude': 'lon', 'prediction_timedelta': 'lead_time'})
    return ds
