"""Climatology models."""
from sheerwater_benchmarking.reanalysis import era5_agg
from sheerwater_benchmarking.utils import dask_remote, cacheable


@dask_remote
@cacheable(data_type='array',
           cache_args=['first_year', 'last_year', 'variable', 'grid', 'mask'],
           chunking={"lat": 121, "lon": 240, "dayofyear": 366},
           auto_rechunk=False)
def climatology(first_year, last_year, variable, grid="global1_5", mask="lsm"):
    """Compute the climatology of the ERA5 data. Years are inclusive."""
    start_time = f"{first_year}-01-01"
    end_time = f"{last_year}-12-31"

    # Get single day, masked data between start and end years
    ds = era5_agg(start_time, end_time, variable=variable,
                  grid=grid, agg=1, mask=mask)

    # Add day of year as a coordinate
    ds = ds.assign_coords(dayofyear=ds.time.dt.dayofyear)

    # Take average over the period to produce climatology
    return ds.groupby('dayofyear').mean(dim='time')


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'grid', 'mask'],
           chunking={"lat": 121, "lon": 240, "dayofyear": 366},
           cache=False,
           auto_rechunk=False)
def climatology_standard_30yr(variable, grid="global1_5", mask="lsm"):
    """Compute the standard 30-year climatology of ERA5 data from 1991-2020."""
    # Get single day, masked data between start and end years
    return climatology(1991, 2020, variable, grid=grid, mask=mask)
