"""Get Tahmo data."""
import numpy as np
import dask.dataframe as dd
import pandas as pd
import xarray as xr

from sheerwater_benchmarking.utils.caching import cacheable
from sheerwater_benchmarking.utils import get_grid_ds, get_grid, get_variable, roll_and_agg, apply_mask, clip_region
from sheerwater_benchmarking.utils.remote import dask_remote

@dask_remote
@cacheable(data_type='tabular', cache_args=['station_code'], cache=True)
def tahmo_station(station_code):
    """Get Tahmo station data."""
    obs = dd.read_csv(f"gs://sheerwater-datalake/tahmo-data/tahmo_qc_stations_v2/{station_code}.csv", on_bad_lines="skip")

    # Get a list of columns starting with quality and merge them
    quality_cols = [col for col in obs.columns if col.startswith('quality')]
    obs['qual'] = obs[quality_cols].mean(axis=1)

    # Get a list of columns starting with pr and merge them
    pr_cols = [col for col in obs.columns if col.startswith('pr')]
    obs['precip'] = obs[pr_cols].mean(axis=1)
    obs['station_code'] = station_code

    # Drop the quality columns
    obs = obs.drop(columns=quality_cols)
    obs = obs.drop(columns=pr_cols)

    return obs


@dask_remote
@cacheable(data_type='array', cache_args=['grid', 'cell_aggregation'],
           chunking={
               'time': 365,
               'lat': 300,
               'lon': 300,
        })
def tahmo_raw(start_time, end_time, grid='global0_25', cell_aggregation='first'):
    """Get tahmo data from the QC controlled stations."""

    # Get the station list
    stat = dd.read_csv("gs://sheerwater-datalake/tahmo-data/tahmo_station_locs.csv")
    stations = stat['station code'].unique()

    files = []
    for station in stations:
        try:
            files.append(tahmo_station(station, backend='parquet', filepath_only=True))
        except FileNotFoundError:
            print(f"File not found for station {station}. Skipping")

    print(files[0].split('/')[:-1])
    path = '/'.join(files[0].split('/')[:-1])
    obs = dd.read_parquet(path)

    # Convert the date column to a datetime
    obs['time'] = dd.to_datetime(obs['Timestamp'])
    obs = obs.drop(['Timestamp'], axis=1)

    # For each station ID roll the data into a daily mean
    obs = obs.groupby([obs.time.dt.date, 'station_code']).agg({'precip': 'sum'})
    obs = obs.reset_index()

    # Round the coordinates to the nearest grid
    lats, lons, grid_size = get_grid(grid)
    # This rounding only works for divisible, uniform grids
    assert (lats[0] % grid_size == 0)
    assert (lons[0] % grid_size == 0)

    def custom_round(x, base):
        return base * round(float(x)/base)

    stat = dd.read_csv("gs://sheerwater-datalake/tahmo-data/tahmo_station_locs.csv")
    stat = stat.rename(columns={'latitude': 'lat', 'longitude': 'lon', 'station code': 'station_code'})
    stat['lat'] = stat['lat'].apply(lambda x: custom_round(x, base=grid_size))
    stat['lon'] = stat['lon'].apply(lambda x: custom_round(x, base=grid_size))

    if cell_aggregation == 'first':
        stations_to_use = stat.groupby(['lat', 'lon']).agg(station_code=('station_code', 'first'))
        stations_to_use = stations_to_use.reset_index()
        stations_to_use = stations_to_use['station_code'].unique()

        stat = stat[stat['station_code'].isin(stations_to_use)]
        stat = stat.set_index('station_code')
        obs = obs.join(stat, on='station_code', how='inner')
    elif cell_aggregation == 'mean':
        # Group by lat/lon/time
        stat = stat.set_index('station_code')
        obs = obs.join(stat, on='station_code', how='inner')
        obs = obs.groupby(by=['lat', 'lon', 'time']).agg(precip=('precip', 'mean'))


    # Prepare for xarray
    obs = obs.drop(['station_code', 'name', 'country'], axis=1)

    # Group by only way to set a multi index in dask?
    obs = obs.groupby(['lat', 'lon', 'time']).agg(precip=('precip', 'mean'))

    print(obs)
    print(obs.columns)

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
           cache_args=['grid', 'agg_days', 'missing_thresh', 'cell_aggregation'],
           chunking={'lat': 300, 'lon': 300, 'time': 365})
def tahmo_rolled(start_time, end_time, agg_days, grid='global0_25', missing_thresh=0.5, cell_aggregation='first'):
    """Tahmo rolled and aggregated."""
    # Get the data
    ds = tahmo_raw(start_time, end_time, grid, cell_aggregation)

    # Roll and agg
    agg_thresh = max(int(agg_days*missing_thresh), 1)

    ds = roll_and_agg(ds, agg=agg_days, agg_col="time", agg_fn='mean', agg_thresh=agg_thresh)

    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'agg_days', 'grid', 'mask', 'region', 'missing_thresh', 'cell_aggregation'],
           chunking={'lat': 300, 'lon': 300, 'time': 365},
           cache=False)
def tahmo(start_time, end_time, variable, agg_days, grid='global0_25', mask='lsm', region='global',
         missing_thresh=0.5, cell_aggregation='first'):
    """Standard interface for ghcn data."""
    ds = tahmo_rolled(start_time, end_time, agg_days, grid, missing_thresh, cell_aggregation)

    # Get the variable
    variable_ghcn = get_variable(variable, 'ghcn')
    variable_sheerwater = get_variable(variable, 'sheerwater')
    ds = ds[variable_ghcn].to_dataset()

    # Apply masking
    ds = apply_mask(ds, mask, var=variable_ghcn, grid=grid)

    # Clip to specified region
    ds = clip_region(ds, region=region)

    # Rename
    ds = ds.rename({variable_ghcn: variable_sheerwater})

    # Note that this is sparse
    ds = ds.assign_attrs(sparse=True)

    return ds