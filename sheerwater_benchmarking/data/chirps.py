"""CHIRPS data product."""
import xarray as xr
from functools import partial
from sheerwater_benchmarking.utils import regrid, dask_remote, cacheable, roll_and_agg, apply_mask, clip_region
from dateutil import parser
from sheerwater_benchmarking.tasks import spw_rainy_onset, spw_precip_preprocess


@dask_remote
@cacheable(data_type='array',
           cache_args=['year', 'grid'],
           chunking={'lat': 300, 'lon': 300, 'time': 365})
def chirps_gridded(year, grid):
    """CHIRPS regridded by year."""
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
    if grid != 'chirps':
        ds = regrid(ds, grid, base='base180', method='conservative')

    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries=['time'],
           cache_args=['grid', 'agg_days'],
           chunking={'lat': 300, 'lon': 300, 'time': 365})
def chirps_rolled(start_time, end_time, agg_days, grid):
    """CHIRPS rolled and aggregated."""
    years = range(parser.parse(start_time).year, parser.parse(end_time).year + 1)

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
           cache_args=['variable', 'agg_days', 'grid', 'mask', 'region'],
           cache=False)
def chirps(start_time, end_time, variable, agg_days, grid='global0_25', mask='lsm', region='global'):
    """Final access function for chirps."""
    if variable not in ['precip', 'rainy_onset', 'rainy_onset_no_drought']:
        raise NotImplementedError("Only precip and derived variables provided by CHIRPS.")

    if variable == 'rainy_onset' or variable == 'rainy_onset_no_drought':
        drought_condition = variable == 'rainy_onset_no_drought'
        fn = partial(chirps_rolled, start_time, end_time, grid=grid)
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
        ds = chirps_rolled(start_time, end_time, agg_days, grid)
        ds = apply_mask(ds, mask, grid=grid)
        ds = clip_region(ds, region=region)

        ds = ds.assign_attrs(sparse=True)

    return ds
