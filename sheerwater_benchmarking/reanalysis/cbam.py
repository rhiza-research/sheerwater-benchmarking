"""Fetches ERA5 data from the Google ARCO Store."""
import xarray as xr
from sheerwater_benchmarking.utils import (dask_remote, cacheable,
                                           apply_mask, clip_region,
                                           roll_and_agg, regrid)

@dask_remote
@cacheable(data_type='array',
           cache_args=['variable'],
           timeseries='time',
           cache=False)
def cbam_raw(start_time, end_time, variable):  # noqa ARG001
    """CBAM raw data with some renaming/averaging."""
    ds = xr.open_zarr('gs://sheerwater-datalake/cbam-data/gap/cbam_20241021.zarr',
                      chunks={'time': 50, 'latitude': 500, 'longitude': 500})

    ds = ds.rename({'date': 'time', 'total_rainfall': 'precip'})
    ds['tmp2m'] = (ds['min_total_temperature'] + ds['max_total_temperature'])/2

    # Raw latitudes are in descending order
    ds = ds.sortby('lat')
    ds = ds[[variable]]

    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'grid'],
           chunking={"lat": 721, "lon": 1440, "time": 30})
def cbam_gridded(start_time, end_time, variable, grid="global1_5"):
    """Aggregates the hourly ERA5 data into daily data.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
            - global0_25: 0.25 degree global grid
    """
    # Read and combine all the data into an array
    ds = cbam_raw(start_time, end_time, variable)
    ds = regrid(ds, grid, base='base180', method='conservative', output_chunks={"lat": 721, "lon": 1440})
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'agg_days', 'grid'],
           chunking={"lat": 721, "lon": 1440, "time": 30},
           cache_disable_if={'agg_days': 1})
def cbam_rolled(start_time, end_time, variable, agg_days=7, grid="global1_5"):
    """Aggregates the hourly ERA5 data into daily data and rolls.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        agg_days (int): The aggregation period, in days.
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
            - global0_25: 0.25 degree global grid
    """
    # Read and combine all the data into an array
    ds = cbam_gridded(start_time, end_time, variable, grid=grid)
    if agg_days == 1:
        return ds
    ds = roll_and_agg(ds, agg=agg_days, agg_col="time", agg_fn="mean")
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'agg_days', 'grid', 'mask', 'region'])
def cbam(start_time, end_time, variable, agg_days, grid='global0_25', mask='lsm', region='global'):
    """Standard format task data for ERA5 Reanalysis.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        agg_days (int): The aggregation period, in days.
        grid (str): The grid resolution to fetch the data at.
        mask (str): The mask to apply to the data.
        region (str): The region to clip the data to.
    """
    # Get daily data
    ds = cbam_rolled(start_time, end_time, variable, agg_days=agg_days, grid=grid)
    # Apply masking
    ds = apply_mask(ds, mask, var=variable, grid=grid)
    # Clip to specified region
    ds = clip_region(ds, region=region)
    return ds
