"""A climatology baseline for benchmarking."""
from sheerwater_benchmarking.utils import dask_remote, cacheable
from sheerwater_benchmarking.reanalysis import era5_agg


@dask_remote
@cacheable(data_type='array',
           cache_args=['first_year', 'last_year', 'variable', 'grid', 'mask'],
           chunking={"lat": 121, "lon": 240, "doy": 366},
           auto_rechunk=True)
def climatology(first_year, last_year, variable, grid="global1_5", mask="lsm"):
    """Compute the climatology of the ERA5 data. Years are inclusive."""
    start_time = f"{first_year}-01-01"
    end_time = f"{last_year}-12-31"

    # Get single day, masked data between start and end years
    ds = era5_agg(start_time, end_time, variable=variable,
                  grid=grid, agg=1, mask=mask)

    # Add day of year as a coordinate
    ds = ds.assign_coords(doy=ds.time.dt.dayofyear)

    # Take average over the period to produce climatology
    return ds.groupby('doy').mean(dim='time')


@dask_remote
@cacheable(data_type='array',
           cache_args=['first_year', 'last_year', 'variable', 'grid', 'mask'],
           chunking={"lat": 121, "lon": 240, "doy": 366},
           auto_rechunk=True)
def climatology_standard_30yr(variable, grid="global1_5", mask="lsm"):
    """Compute the standard 30-year climatology of ERA5 data from 1991-2020."""
    # Get single day, masked data between start and end years
    ds = era5_agg("1991-01-01", "2020-12-31", variable=variable,
                  grid=grid, agg=1, mask=mask)

    # Add day of year as a coordinate
    ds = ds.assign_coords(doy=ds.time.dt.dayofyear)

    # Take average over the period to produce climatology
    return ds.groupby('doy').mean(dim='time')
