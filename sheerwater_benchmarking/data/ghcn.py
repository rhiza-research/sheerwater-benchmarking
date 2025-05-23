"""Get GHCND data."""
import pandas as pd
from functools import partial
from dateutil import parser
import numpy as np
import dask.dataframe as dd
import dask
import xarray as xr

from sheerwater_benchmarking.utils.caching import cacheable
from sheerwater_benchmarking.utils import get_grid_ds, get_grid, get_variable, roll_and_agg, apply_mask, clip_region
from sheerwater_benchmarking.utils.time_utils import generate_dates_in_between
from sheerwater_benchmarking.utils.remote import dask_remote
from sheerwater_benchmarking.tasks import spw_rainy_onset, spw_precip_preprocess


@cacheable(data_type='tabular', cache_args=[])
def station_list():
    """Gets GHCN station metadata."""
    df = pd.read_table('https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt',
                       sep="\\s+", names=['ghcn_id', 'lat', 'lon', 'unknown', 'start_year', 'end_year'])

    df = df.drop(['unknown'], axis=1)
    df = df.groupby(by=['ghcn_id']).first().reset_index()

    return df


@cacheable(data_type='array', cache_args=['ghcn_id', 'drop_flagged'], cache=True, timeseries='time')
def ghcnd_station(start_time, end_time, ghcn_id, drop_flagged=True, grid='global0_25'):  # noqa:  ARG001
    """Get GHCNd observed data timeseries for a single station.

    Global Historical Climatology Network - Daily.

    Args:
        start_time (str): omit data before this date
        end_time (str): omit data after this date
        drop_flagged (bool): drops all flagged data
        grid (str): Grid to put the station on
        ghcn_id (str): GHCND station ID

    Returns:
        pd.DataFrame | list[pd.DataFrame]: observed data timeseries with
        columns `time`, `precip`, `tmin`, `tmax`, `temp`
    """
    obs = pd.read_csv(f"s3://noaa-ghcn-pds/csv.gz/by_station/{ghcn_id}.csv.gz",
                      compression='gzip',
                      names=['ghcn_id', 'date', 'variable', 'value', 'mflag', 'qflag', 'sflag', 'otime'],
                      dtype={'ghcn_id': str,
                             'date': str,
                             'variable': str,
                             'value': int,
                             'mflag': str,
                             'qflag': str,
                             'sflag': str,
                             'otime': str},
                      storage_options={'anon': True})

    # Drop rows we don't care about
    obs = obs[obs['variable'].isin(['TMAX', 'TMIN', 'TAVG', 'PRCP'])]

    if drop_flagged:
        obs = obs[obs['qflag'].isna()]

    # Rplace any invalid data
    INVALID_NUMBER = 9999
    obs.replace(INVALID_NUMBER, pd.NA, inplace=True)

    # Divide by 10 because data is represented in 10ths
    obs['value'] = obs['value'] / 10.0
    # Assign to new column based on variable values
    obs['tmax'] = obs.apply(lambda x: x.value if x['variable'] == 'TMAX' else pd.NA, axis=1)
    obs['tmin'] = obs.apply(lambda x: x.value if x['variable'] == 'TMIN' else pd.NA, axis=1)
    obs['temp'] = obs.apply(lambda x: x.value if x['variable'] == 'TAVG' else pd.NA, axis=1)
    obs['precip'] = obs.apply(lambda x: x.value if x['variable'] == 'PRCP' else pd.NA, axis=1)

    if not drop_flagged:
        obs['tmax_q'] = obs.apply(lambda x: x.qflag if x['variable'] == 'TMAX' else pd.NA, axis=1)
        obs['tmin_q'] = obs.apply(lambda x: x.qflag if x['variable'] == 'TMIN' else pd.NA, axis=1)
        obs['temp_q'] = obs.apply(lambda x: x.qflag if x['variable'] == 'TAVG' else pd.NA, axis=1)
        obs['precip_q'] = obs.apply(lambda x: x.qflag if x['variable'] == 'PRCP' else pd.NA, axis=1)

    obs = obs.drop(['variable', 'value', 'sflag', 'mflag', 'qflag', 'otime'], axis=1)

    # Group by date and merge columns
    obs = obs.groupby(by=['date']).first()
    obs = obs.reset_index()

    # If temp is none avrage the two
    atemp = (obs['tmin'] + obs['tmax'])/2
    obs['temp'] = obs['temp'].astype(float).fillna(atemp.astype(float))

    # Set all variables to floats
    obs.temp = obs.temp.astype(np.float32)
    obs.tmax = obs.tmax.astype(np.float32)
    obs.tmin = obs.tmin.astype(np.float32)
    obs.precip = obs.precip.astype(np.float32)

    if not drop_flagged:
        obs.temp_q = obs.temp_q.astype(str)
        obs.tmax_q = obs.tmax_q.astype(str)
        obs.tmin_q = obs.tmin_q.astype(str)
        obs.precip_q = obs.precip_q.astype(str)

    # Convert date into a datetime
    obs["time"] = pd.to_datetime(obs["date"])
    obs = obs.drop(['date'], axis=1)

    # Round the coordinates to the nearest grid
    lats, lons, grid_size = get_grid(grid)

    # This rounding only works for divisible, uniform grids
    assert (lats[0] % grid_size == 0)
    assert (lons[0] % grid_size == 0)

    def custom_round(x, base):
        return base * round(float(x)/base)

    # Get the lat and lon and round them
    ghcn_id = obs['ghcn_id'].iloc[0]

    stat = station_list()
    stat = stat[stat['ghcn_id'] == ghcn_id]

    lat = custom_round(stat['lat'].iloc[0], base=grid_size)
    lon = custom_round(stat['lon'].iloc[0], base=grid_size)

    obs = obs.set_index(['ghcn_id', 'time'])
    obs = obs.to_xarray()

    obs = obs.assign_coords({'lat': lat, 'lon': lon})

    # slice
    obs = obs.sel(time=slice(start_time, end_time))

    # Fill out the time dimension from start to end date even if nan
    day = generate_dates_in_between(start_time, end_time, date_frequency='daily')
    obs = obs.reindex({'time': day})

    return obs


@dask_remote
@cacheable(data_type='array', cache_args=['year', 'grid', 'cell_aggregation'],
           chunking={
               'time': 365,
               'lat': 300,
               'lon': 300,
})
def ghcnd_yearly(year, grid='global0_25', cell_aggregation='first'):
    """Get a by year station data and save it as a zarr."""
    obs = dd.read_csv(f"s3://noaa-ghcn-pds/csv/by_year/{year}.csv",
                      names=['ghcn_id', 'date', 'variable', 'value', 'mflag', 'qflag', 'sflag', 'otime'],
                      header=0,
                      blocksize="32MB",
                      dtype={'ghcn_id': str,
                             'date': str,
                             'variable': str,
                             'value': int,
                             'mflag': str,
                             'qflag': str,
                             'sflag': str,
                             'otime': str},
                      storage_options={'anon': True},
                      on_bad_lines="skip")

    # Drop rows we don't care about
    obs = obs[obs['variable'].isin(['TMAX', 'TMIN', 'TAVG', 'PRCP'])]

    # Drop flagged data
    obs = obs[obs['qflag'].isna()]

    # Rplace any invalid data
    INVALID_NUMBER = 9999
    obs = obs.replace(INVALID_NUMBER, pd.NA)

    # Divide by 10 because data is represented in 10ths
    obs['value'] = obs['value'] / 10.0

    # Assign to new column based on variable values
    obs['tmax'] = obs.apply(lambda x: x.value if x['variable'] == 'TMAX' else pd.NA,
                            axis=1, meta=('tmax', 'f8'))
    obs['tmin'] = obs.apply(lambda x: x.value if x['variable'] == 'TMIN' else pd.NA,
                            axis=1, meta=('tmin', 'f8'))
    obs['temp'] = obs.apply(lambda x: x.value if x['variable'] == 'TAVG' else pd.NA,
                            axis=1, meta=('temp', 'f8'))
    obs['precip'] = obs.apply(lambda x: x.value if x['variable'] == 'PRCP' else pd.NA,
                              axis=1, meta=('precip', 'f8'))

    obs = obs.drop(['variable', 'value', 'qflag'], axis=1)

    # Group by date and merge columns
    obs = obs.groupby(by=['date', 'ghcn_id']).first()
    obs = obs.reset_index()

    # If temp is none avrage the two
    atemp = (obs['tmin'] + obs['tmax'])/2
    obs['temp'] = obs['temp'].astype(float).fillna(atemp.astype(float))

    # Convert date into a datetime
    obs["time"] = dd.to_datetime(obs["date"])
    obs = obs.drop(['date'], axis=1)

    # Round the coordinates to the nearest grid
    lats, lons, grid_size = get_grid(grid)

    # This rounding only works for divisible, uniform grids
    assert (lats[0] % grid_size == 0)
    assert (lons[0] % grid_size == 0)

    def custom_round(x, base):
        return base * round(float(x)/base)

    stat = station_list()
    stat['lat'] = stat['lat'].apply(lambda x: custom_round(x, base=grid_size))
    stat['lon'] = stat['lon'].apply(lambda x: custom_round(x, base=grid_size))

    stat = stat.set_index('ghcn_id')
    obs = obs.join(stat, on='ghcn_id', how='inner')

    if cell_aggregation == 'first':
        stations_to_use = obs.groupby(['lat', 'lon']).agg(ghcn_id=('ghcn_id', 'first'))
        stations_to_use = stations_to_use['ghcn_id'].unique()

        obs = obs[obs['ghcn_id'].isin(stations_to_use)]
    elif cell_aggregation == 'mean':
        # Group by lat/lon/time
        obs = obs.groupby(by=['lat', 'lon', 'time']).agg(temp=('temp', 'mean'),
                                                         precip=('precip', 'mean'),
                                                         tmin=('tmin', 'min'),
                                                         tmax=('tmax', 'max'))

    obs.temp = obs.temp.astype(np.float32)
    obs.tmax = obs.tmax.astype(np.float32)
    obs.tmin = obs.tmin.astype(np.float32)
    obs.precip = obs.precip.astype(np.float32)

    # Convert to xarray - for this to succeed obs must be a pandas dataframe
    obs = xr.Dataset.from_dataframe(obs.compute())

    # Reindex to fill out the lat/lon
    grid_ds = get_grid_ds(grid)
    obs = obs.reindex_like(grid_ds)

    # Return the xarray
    return obs


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['grid', 'cell_aggregation'],
           chunking={'lat': 300, 'lon': 300, 'time': 365})
def ghcnd(start_time, end_time, grid="global0_25", cell_aggregation='first'):
    """Final gridded station data before aggregation."""
    # Get years between start time and end time
    years = range(parser.parse(start_time).year, parser.parse(end_time).year + 1)

    datasets = []
    for year in years:
        ds = dask.delayed(ghcnd_yearly)(year, grid, cell_aggregation, filepath_only=True)
        datasets.append(ds)

    datasets = dask.compute(*datasets)

    x = xr.open_mfdataset(datasets,
                          engine='zarr',
                          parallel=True,
                          chunks={'lat': 300, 'lon': 300, 'time': 365})

    return x


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['grid', 'agg_days', 'missing_thresh', 'cell_aggregation'],
           validate_cache_timeseries=False,
           chunking={'lat': 300, 'lon': 300, 'time': 365})
def ghcnd_rolled(start_time, end_time, agg_days,
                 grid='global0_25', missing_thresh=0.5, cell_aggregation='first'):
    """GHCND rolled and aggregated."""
    # Get the data
    ds = ghcnd(start_time, end_time, grid, cell_aggregation)

    # Roll and agg
    agg_thresh = max(int(agg_days*missing_thresh), 1)

    ds = roll_and_agg(ds, agg=agg_days, agg_col="time", agg_fn='mean', agg_thresh=agg_thresh)
    return ds


@dask_remote
def _ghcn_rolled_unified(start_time, end_time, variable, agg_days,
                         grid='global0_25', missing_thresh=0.5, cell_aggregation='mean'):
    """Standard interface for ghcn data."""
    ds = ghcnd_rolled(start_time, end_time, agg_days=agg_days, grid=grid,
                      missing_thresh=missing_thresh, cell_aggregation=cell_aggregation)

    # Get the variable
    variable_ghcn = get_variable(variable, 'ghcn')
    variable_sheerwater = get_variable(variable, 'sheerwater')
    ds = ds[variable_ghcn].to_dataset()
    # Rename
    ds = ds.rename({variable_ghcn: variable_sheerwater})
    # Note that this is sparse
    ds = ds.assign_attrs(sparse=True)
    return ds


@dask_remote
def _ghcn_unified(start_time, end_time, variable, agg_days,
                  grid='global0_25', mask='lsm', region='global',
                  missing_thresh=0.5, cell_aggregation='first'):
    """Standard interface for ghcn data."""
    if variable == 'rainy_onset' or variable == 'rainy_onset_no_drought':
        drought_condition = variable == 'rainy_onset_no_drought'
        fn = partial(_ghcn_rolled_unified, start_time, end_time, variable='precip', grid=grid,
                     missing_thresh=missing_thresh, cell_aggregation=cell_aggregation)
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
        ds = _ghcn_rolled_unified(start_time, end_time, variable=variable, agg_days=agg_days,
                                  grid=grid, missing_thresh=missing_thresh, cell_aggregation=cell_aggregation)
        # Apply masking
        ds = apply_mask(ds, mask, grid=grid)
        ds = clip_region(ds, region=region)
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'agg_days', 'grid', 'mask', 'region', 'missing_thresh'],
           chunking={'lat': 300, 'lon': 300, 'time': 365},
           cache=False)
def ghcn(start_time, end_time, variable, agg_days,
         grid='global0_25', mask='lsm', region='global',
         missing_thresh=0.5):
    """Standard interface for ghcn data."""
    return _ghcn_unified(start_time, end_time, variable, agg_days=agg_days,
                         grid=grid, mask=mask, region=region,
                         missing_thresh=missing_thresh, cell_aggregation='first')


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'agg_days', 'grid', 'mask', 'region', 'missing_thresh'],
           chunking={'lat': 300, 'lon': 300, 'time': 365},
           cache=False)
def ghcn_avg(start_time, end_time, variable, agg_days, grid='global0_25', mask='lsm', region='global',
             missing_thresh=0.5):
    """Standard interface for ghcn data."""
    return _ghcn_unified(start_time, end_time, variable, agg_days=agg_days,
                         grid=grid, mask=mask, region=region,
                         missing_thresh=missing_thresh, cell_aggregation='mean')
