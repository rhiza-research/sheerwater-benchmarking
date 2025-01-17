"""Imerg data product."""
import xarray as xr
import gcsfs

from sheerwater_benchmarking.utils import cacheable, dask_remote, regrid, roll_and_agg, apply_mask, clip_region
from dateutil import parser

@dask_remote
@cacheable(data_type='array',
           cache_args=['year'],
           chunking={'lat': 300, 'lon': 300, 'time': 365})
def imerg_raw(year):
    """Concatted imerge netcdf files by year."""
    fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
    gsf = [fs.open(x) for x in fs.glob(f'gs://sheerwater-datalake/imerg/{year}*.nc')]

    ds = xr.open_mfdataset(gsf, engine='h5netcdf')

    return ds

@dask_remote
@cacheable(data_type='array',
           cache_args=['grid'],
           chunking={'lat': 300, 'lon': 300, 'time': 365})
def imerg_gridded(start_time, end_time, grid):
    """Regridded version of whole imerg dataset."""
    years = range(parser.parse(start_time).year, parser.parse(end_time).year + 1)

    datasets = []
    for year in years:
        ds = imerg_raw(year, filepath_only=True)
        datasets.append(ds)

    ds = xr.open_mfdataset(datasets,
                          engine='zarr',
                          parallel=True,
                          chunks={'lat': 300, 'lon': 300, 'time': 365})

    ds = ds['precipitation'].to_dataset()
    ds = ds.rename({'precipitation': 'precip'})

    # regrid
    ds = regrid(ds, grid, base='base180', method='conservative')

    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries=['time'],
           cache_args=['grid', 'agg_days'],
           chunking={'lat': 300, 'lon': 300, 'time': 365})
def imerg_rolled(start_time, end_time, agg_days, grid):
    """Imerg rolled and agged."""
    ds = imerg_gridded(start_time, end_time, grid)
    ds = roll_and_agg(ds, agg=agg_days, agg_col="time", agg_fn='mean')
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries=['time'],
           cache_args=['variable', 'agg_days', 'grid', 'mask', 'region'],
           cache=False)
def imerg(start_time, end_time, variable, agg_days, grid='global0_25', mask='lsm', region='global'):
    """Final imerg product."""
    if variable != 'precip':
        raise NotImplementedError("Only precip provided by imerg.")

    ds = imerg_rolled(start_time, end_time, agg_days, grid)

    # Apply masking
    ds = apply_mask(ds, mask, var=variable, grid=grid)

    # Clip to specified region
    ds = clip_region(ds, region=region)

    return ds
