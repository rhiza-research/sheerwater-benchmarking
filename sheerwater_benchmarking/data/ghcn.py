"""Get GHCND data."""
import pandas as pd
import dask.dataframe as dd

from sheerwater_benchmarking.utils.caching import cacheable
from sheerwater_benchmarking.utils.remote import dask_remote


@cacheable(data_type='tabular', cache_args=[])
def station_list():
    """Gets GHCN station metadata."""
    df = pd.read_table('https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt',
                       sep="\\s+", names=['ghcnd_id', 'lat', 'lon', 'unknown', 'start_year', 'end_year'])

    df = df.drop(['unknown'], axis=1)
    df = df.groupby(by=['ghcnd_id']).first().reset_index()

    return df

@cacheable(data_type='tabular', cache_args=['ghcnd_id', 'drop_flagged'], cache=False, timeseries='time')
def ghcnd_station(start_time, end_time, ghcnd_id, drop_flagged=True): # noqa:  ARG001
    """Get GHCNd observed data timeseries for a single station.

    Global Historical Climatology Network - Daily.

    Args:
        start_time (str): omit data before this date
        end_time (str): omit data after this date
        drop_flagged (bool): drops all flagged data
        ghcnd_id (str): GHCND station ID

    Returns:
        pd.DataFrame | list[pd.DataFrame]: observed data timeseries with
        columns `time`, `precip`, `tmin`, `tmax`, `temp`
    """
    obs = pd.read_csv(f"s3://noaa-ghcn-pds/csv.gz/by_station/{ghcnd_id}.csv.gz",
                      compression='gzip',
                      names=['ghcn_id', 'date', 'variable', 'value', 'mflag', 'qflag', 'sflag', 'otime'],
                      dtype={'ghcn_id': str,
                             'date': str,
                             'variable':str,
                             'value':int,
                             'mflag':str,
                             'qflag':str,
                             'sflag':str,
                             'otime':str},
                      storage_options={'anon': True})

    # Drop rows we don't care about
    obs = obs[obs['variable'].isin(['TMAX', 'TMIN', 'TAVG', 'PRCP'])]

    if drop_flagged:
        obs = obs[obs['qflag'].isna()]

    # Rplace any invalid data
    INVALID_NUMBER = 9999
    obs.replace(INVALID_NUMBER, pd.NA, inplace=True)

    # Assign to new column based on variable values
    obs['value'] = obs['value'] / 10.0
    obs['tmax'] = obs.apply(lambda x: x.value if x['variable'] == 'TMAX' else pd.NA, axis=1)
    obs['tmin'] = obs.apply(lambda x: x.value if x['variable'] == 'TMIN' else pd.NA, axis=1)
    obs['temp'] = obs.apply(lambda x: x.value if x['variable'] == 'TAVG' else pd.NA, axis=1)
    obs['precip'] = obs.apply(lambda x: x.value if x['variable'] == 'PRCP' else pd.NA, axis=1)

    if not drop_flagged:
        obs['tmax-q'] = obs.apply(lambda x: x.qflag if x['variable'] == 'TMAX' else pd.NA, axis=1)
        obs['tmin-q'] = obs.apply(lambda x: x.qflag if x['variable'] == 'TMIN' else pd.NA, axis=1)
        obs['temp-q'] = obs.apply(lambda x: x.qflag if x['variable'] == 'TAVG' else pd.NA, axis=1)
        obs['precip-q'] = obs.apply(lambda x: x.qflag if x['variable'] == 'PRCP' else pd.NA, axis=1)

    obs = obs.drop(['variable', 'value', 'sflag', 'mflag', 'qflag', 'otime'], axis=1)

    # Group by date and merge columns
    obs = obs.groupby(by=['date']).first()
    obs = obs.reset_index()

    # If temp is none avrage the two
    atemp = (obs['tmin'] + obs['tmax'])/2
    obs['temp'] = obs['temp'].astype(float).fillna(atemp.astype(float))

    # Set all variables to floats
    obs.temp = obs.temp.astype(float)
    obs.tmax = obs.tmax.astype(float)
    obs.tmin = obs.tmin.astype(float)
    obs.precip = obs.precip.astype(float)

    # Convert date into a datetime
    obs["time"] = pd.to_datetime(obs["date"])
    obs = obs.drop(['date'], axis=1)

    return obs


@dask_remote
@cacheable(data_type='tabular', cache_args=[], timeseries='time', backend='parquet')
def ghcnd(start_time, end_time): # noqa:  ARG001
    """Get GHCNd observed data timeseries for a single station.

    Global Historical Climatology Network - Daily.

    Args:
        start_time (str): omit data before this date
        end_time (str): omit data after this date

    Returns:
        dd.DataFrame observed data timeseries with
        columns `time`, `precip`, `tmin`, `tmax`, `temp`
    """
    obs = dd.read_csv("s3://noaa-ghcn-pds/csv.gz/by_station/*.csv.gz",
                                    compression='gzip',
                                    names=['ghcn_id', 'date', 'variable', 'value', 'mflag', 'qflag', 'sflag', 'otime'],
                                    dtype={'ghcn_id': str,
                                           'date': str,
                                           'variable':str,
                                           'value':int,
                                           'mflag':str,
                                           'qflag':str,
                                           'sflag':str,
                                           'otime':str},
                                    storage_options={'anon': True},
                                    blocksize=None,
                                    sample=False,
                                    on_bad_lines="skip")

    # Drop rows we don't care about
    obs = obs[obs['variable'].isin(['TMAX', 'TMIN', 'TAVG', 'PRCP'])]

    # Rplace any invalid data
    INVALID_NUMBER = 9999
    obs = obs.replace(INVALID_NUMBER, pd.NA)

    # Assign to new column based on variable values
    obs['value'] = obs['value'] / 10.0
    obs['tmax'] = obs.apply(lambda x: x.value if x['variable'] == 'TMAX' else pd.NA,
                            axis=1, meta=('tmax', 'f8'))
    obs['tmin'] = obs.apply(lambda x: x.value if x['variable'] == 'TMIN' else pd.NA,
                            axis=1, meta=('tmin', 'f8'))
    obs['temp'] = obs.apply(lambda x: x.value if x['variable'] == 'TAVG' else pd.NA,
                            axis=1, meta=('temp', 'f8'))
    obs['precip'] = obs.apply(lambda x: x.value if x['variable'] == 'PRCP' else pd.NA,
                              axis=1, meta=('precip', 'f8'))

    obs['tmax-q'] = obs.apply(lambda x: x.qflag if x['variable'] == 'TMAX' else pd.NA,
                              axis=1, meta=('tmax-q', 'str'))
    obs['tmin-q'] = obs.apply(lambda x: x.qflag if x['variable'] == 'TMIN' else pd.NA,
                              axis=1, meta=('tmin-q', 'str'))
    obs['temp-q'] = obs.apply(lambda x: x.qflag if x['variable'] == 'TAVG' else pd.NA,
                              axis=1, meta=('temp-q', 'str'))
    obs['precip-q'] = obs.apply(lambda x: x.qflag if x['variable'] == 'PRCP' else pd.NA,
                                axis=1, meta=('precip-q', 'str'))

    obs = obs.drop(['variable', 'value', 'qflag'], axis=1)

    # Group by date and merge columns
    obs = obs.groupby(by=['date', 'ghcn_id']).first()
    obs = obs.reset_index()

    # If temp is none avrage the two
    atemp = (obs['tmin'] + obs['tmax'])/2
    obs['temp'] = obs['temp'].astype(float).fillna(atemp.astype(float))

    # Set all variables to floats
    obs.temp = obs.temp.astype(float)
    obs.tmax = obs.tmax.astype(float)
    obs.tmin = obs.tmin.astype(float)
    obs.precip = obs.precip.astype(float)

    # Convert date into a datetime
    obs["time"] = dd.to_datetime(obs["date"])
    obs["year"] = obs["time"].dt.year
    obs = obs.drop(['date'], axis=1)

    obs = obs.repartition(partition_size="100MB")
    obs.cache_partition = ['year']

    return obs
