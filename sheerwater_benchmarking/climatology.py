"""Climatology models."""
from sheerwater_benchmarking.masks import land_sea_mask
import dask
from sheerwater_benchmarking.reanalysis import era5_daily
from sheerwater_benchmarking.utils import (dask_remote, cacheable, clip_region,
                                           apply_mask, lon_base_change)


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'first_year', 'last_year', 'grid'],
           chunking={"lat": 721, "lon": 1440, "dayofyear": 366},
           auto_rechunk=False)
def climatology_raw(variable, first_year, last_year, grid='global1_5'):
    """Compute the climatology of the ERA5 data. Years are inclusive."""
    start_time = f"{first_year}-01-01"
    end_time = f"{last_year}-12-31"

    # Get single day, masked data between start and end years
    ds = era5_daily(start_time, end_time, variable=variable, grid=grid)

    # Add day of year as a coordinate
    ds = ds.assign_coords(dayofyear=ds.time.dt.dayofyear)

    # Take average over the period to produce climatology
    return ds.groupby('dayofyear').mean(dim='time')


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'first_year', 'last_year', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "dayofyear": 366},
           cache=False)
def climatology(variable, first_year=1991, last_year=2020, grid="global1_5", mask='lsm', region='global'):
    """Compute the standard 30-year climatology of ERA5 data from 1991-2020."""
    # Get single day, masked data between start and end years
    ds = climatology_raw(variable, first_year, last_year, grid=grid)

    # Apply mask
    if mask != 'lsm' and mask is not None:
        raise NotImplementedError("Only land-sea or no mask is implemented.")
    mask_ds = land_sea_mask(grid=grid).compute() if mask == "lsm" else None
    ds = apply_mask(ds, mask_ds, variable)

    # Clip to region, suppressing large chunk splitting
    with dask.config.set(**{'array.slicing.split_large_chunks': False}):
        ds = clip_region(ds, region)
    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "dayofyear": 366},
           cache=False,
           auto_rechunk=False)
def climatology_standard_30yr(variable, grid="global1_5", mask="lsm", region='global'):
    """Compute the standard 30-year climatology of ERA5 data from 1991-2020."""
    # Get single day, masked data between start and end years
    return climatology(variable, 1991, 2020, grid=grid, mask=mask, region=region)


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'grid'],
           chunking={"lat": 121, "lon": 240, "dayofyear": 366, "time": 30},
           cache=True,
           auto_rechunk=False)
def climatology_rolling_raw(variable, grid="global1_5"):
    """Compute ."""
    # Get single day, masked data between start and end years
    pass


# @dask_remote
# @cacheable(data_type='array',
#            timeseries='time',
#            cache=False,
#            cache_args=['variable', 'grid', 'mask', 'region'],
#            chunking={"lat": 121, "lon": 240, "dayofyear": 366, "time": 30},
#            auto_rechunk=False)
# def climatology_rolling(start_time, end_time, variable, grid="global1_5", mask="lsm", region='global'):
#     """Compute the standard 30-year climatology of ERA5 data from 1991-2020."""
#     # Get single day, masked data between start and end years
#     return climatology(variable, 1991, 2020, grid=grid, mask=mask, region=region)


__all__ = ['climatology', 'climatology_standard_30yr']
