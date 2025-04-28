"""Imerg data product."""
import xarray as xr
import gcsfs
from dateutil import parser
from functools import partial

from sheerwater_benchmarking.utils import cacheable, dask_remote, regrid, roll_and_agg, apply_mask, clip_region
from sheerwater_benchmarking.tasks import spw_rainy_onset, spw_precip_preprocess


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
    if grid != 'imerg':
        ds = regrid(ds, grid, base='base180', method='conservative')

    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries=['time'],
           cache_args=['grid', 'agg_days'],
           chunking={'lat': 300, 'lon': 300, 'time': 365})
def imerg_rolled(start_time, end_time, agg_days, grid):
    """Imerg rolled and aggregated."""
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
    if variable not in ['precip', 'rainy_onset', 'rainy_onset_no_drought']:
        raise NotImplementedError("Only precip and derived variables provided by IMERG.")

    if variable == 'rainy_onset' or variable == 'rainy_onset_no_drought':
        drought_condition = variable == 'rainy_onset_no_drought'
        fn = partial(imerg_rolled, start_time, end_time, grid=grid)
        roll_days = [8, 11] if not drought_condition else [8, 11, 11]
        shift_days = [0, 0] if not drought_condition else [0, 0, 11]
        data = spw_precip_preprocess(fn, agg_days=roll_days, shift_days=shift_days,
                                     mask=mask, region=region, grid=grid)
        ds = spw_rainy_onset(data,
                             onset_group=['ea_rainy_season', 'year'], aggregate_group=None,
                             time_dim='time', prob_type='deterministic',
                             drought_condition=drought_condition,
                             mask=mask, region=region, grid=grid)
        # Rainy onset is sparse, so we need to set the sparse attribute
        ds = ds.assign_attrs(sparse=True)
    else:
        ds = imerg_rolled(start_time, end_time, agg_days=agg_days, grid=grid)
        ds = apply_mask(ds, mask, grid=grid)
        ds = clip_region(ds, region=region)

    return ds
