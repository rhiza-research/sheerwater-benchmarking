import xarray as xr

from sheerwater_benchmarking.utils import regrid, dask_remote, cacheable, roll_and_agg, apply_mask, clip_region
from dateutil import parser

@dask_remote
@cacheable(data_type='array',
           cache_args=['year', 'grid'],
           chunking={'lat': 300, 'lon': 300, 'time': 365})
def chirps_gridded(year, grid):
    # Open the datastore
    store = 'https://nyu1.osn.mghpcc.org/leap-pangeo-pipeline/chirps_feedstock/chirps-global-daily.zarr'
    ds = xr.open_dataset(store, engine='zarr', chunks={})

    # clip to the year
    start_time = str(year) + '-01-01'
    end_time = str(year) + '-12-31'

    ds = ds.sel({'time': slice(start_time, end_time)})

    # Rename to lat/lon
    ds = ds.rename({'latitude': 'lat', 'longitude': 'lon'})

    # regrid
    ds = regrid(ds, grid, base='base180', method='conservative')

    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries=['time'],
           cache_args=['grid', 'agg_days'],
           chunking={'lat': 300, 'lon': 300, 'time': 365})
def chirps_rolled(start_time, end_time, agg_days, grid):
    years = []
    current_year = parser.parse(start_time).year

    while current_year <= parser.parse(end_time).year:
        years.append(current_year)
        current_year += 1

    datasets = []
    for year in years:
        ds = chirps_gridded(year, grid, filepath_only=True)
        datasets.append(ds)

    ds = xr.open_mfdataset(datasets,
                          engine='zarr',
                          parallel=True,
                          chunks={'lat': 300, 'lon': 300, 'time': 365})

    ds = roll_and_agg(ds, agg=agg_days, agg_col="time", agg_fn='mean')

    return ds

@dask_remote
@cacheable(data_type='array',
           timeseries=['time'],
           cache_args=[],
           cache=False)
def chirps(start_time, end_time, variable, agg_days, grid='global0_25', region='global', mask='lsm'):
    ds = chirps_rolled(start_time, end_time, agg_days, grid)

    # Apply masking
    ds = apply_mask(ds, mask, var=variable, grid=grid)

    # Clip to specified region
    ds = clip_region(ds, region=region)

    return ds



