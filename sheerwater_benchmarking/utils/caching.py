"""Automated dataframe caching utilities."""
import datetime
import dateparser
from functools import wraps
from inspect import signature, Parameter
import logging
import hashlib

import gcsfs
from deltalake import DeltaTable, write_deltalake
from rasterio.io import MemoryFile
from rasterio.enums import Resampling
from google.cloud import storage
import terracotta as tc
import sqlalchemy
import pickle

import numpy as np
import pandas as pd
import dask.dataframe as dd
import xarray as xr

from sheerwater_benchmarking.utils.data_utils import lon_base_change
from sheerwater_benchmarking.utils.secrets import postgres_write_password, postgres_read_password

logger = logging.getLogger(__name__)

CHUNK_SIZE_UPPER_LIMIT_MB = 300
CHUNK_SIZE_LOWER_LIMIT_MB = 30


def get_chunk_size(ds, size_in='MB'):
    """Get the chunk size of a dataset in MB or number of chunks.

    Args:
        ds (xr.Dataset): The dataset to get the chunk size of.
        size_in (str): The size to return the chunk size in. One of:
            'KB', 'MB', 'GB', 'TB' for kilo, mega, giga, and terabytes respectively.
    """
    chunk_groups = [(dim, np.median(chunks)) for dim, chunks in ds.chunks.items()]
    div = {'KB': 10**3, 'MB': 10**6, 'GB': 10**9, 'TB': 10**12}[size_in]
    chunk_sizes = [x[1] for x in chunk_groups]
    return np.prod(chunk_sizes) * 4 / div, chunk_groups


def merge_chunk_by_arg(chunking, chunk_by_arg, kwargs):
    """Merge chunking and chunking modifiers into a single chunking dict.

    Args:
        chunking (dict): The chunking to merge.
        chunk_by_arg (dict): The chunking modifiers to merge.
        kwargs (dict): The kwargs to check for chunking modifiers.
    """
    if chunk_by_arg is None:
        return chunking

    for k in chunk_by_arg:
        if k not in kwargs:
            raise ValueError(f"Chunking modifier {k} not found in kwargs.")

        if kwargs[k] in chunk_by_arg[k]:
            # If argument value in chunk_by_arg then merge the chunking
            chunk_dict = chunk_by_arg[k][kwargs[k]]
            chunking.update(chunk_dict)

    return chunking


def prune_chunking_dimensions(ds, chunking):
    """Prune the chunking dimensions to only those that exist in the dataset.

    Args:
        ds (xr.Dataset): The dataset to check for chunking dimensions.
        chunking (dict): The chunking dimensions to prune.
    """
    # Get the chunks for the dataset
    ds_chunks = {dim: ds.chunks[dim][0] for dim in ds.chunks}

    # Drop any dimensions that don't exist in the ds_chunks
    dims_to_drop = []
    for dim in chunking:
        if dim not in ds_chunks:
            dims_to_drop.append(dim)

    for dim in dims_to_drop:
        del chunking[dim]

    return chunking


def chunking_compare(ds, chunking):
    """Compare the chunking of a dataset to a specified chunking.

    Args:
        ds (xr.Dataset): The dataset to check the chunking of.
        chunking (dict): The chunking to compare to.
    """
    # Get the chunks for the dataset
    ds_chunks = {dim: ds.chunks[dim][0] for dim in ds.chunks}
    chunking = prune_chunking_dimensions(ds, chunking)
    return ds_chunks == chunking


def drop_encoded_chunks(ds):
    """Drop the encoded chunks from a dataset."""
    for var in ds.data_vars:
        if 'chunks' in ds[var].encoding:
            del ds[var].encoding['chunks']
        if 'preferred_chunks' in ds[var].encoding:
            del ds[var].encoding['preferred_chunks']

    for coord in ds.coords:
        if 'chunks' in ds[coord].encoding:
            del ds[coord].encoding['chunks']
        if 'preferred_chunks' in ds[coord].encoding:
            del ds[coord].encoding['preferred_chunks']

    return ds


def check_cache_disable_if(cache_disable_if, cache_arg_values):
    """Check if the cache should be disabled for the given kwargs.

    Cache disable if is a dict or list of dicts. Each dict specifies a set of
    arguments that should disable the cache if they are present.
    """
    if cache_disable_if is None:
        return False

    if isinstance(cache_disable_if, dict):
        cache_disable_if = [cache_disable_if]

    for d in cache_disable_if:
        if not isinstance(d, dict):
            raise ValueError("cache_disable_if only accepts a dict or list of dicts.")

        # Get the common keys
        common_keys = set(cache_arg_values).intersection(d)

        # Remove any args not passed
        comp_arg_values = {key: cache_arg_values[key] for key in common_keys}
        d = {key: d[key] for key in common_keys}

        # Iterate through each key and check if the values match, with support for lists
        key_match = [
            (not isinstance(d[k], list) and comp_arg_values[k] == d[k]) or
            (isinstance(d[k], list) and comp_arg_values[k] in d[k])
            for k in common_keys
        ]
        # Within a cache disable if dict, if all keys match, disable the cache
        if all(key_match):
            print(f"Caching disabled for arg values {d}")
            return False
    # Keep the cache enabled - we didn't find a match
    return True


def read_from_delta(cache_path):
    """Read from a deltatable into a pandas dataframe."""
    return DeltaTable(cache_path).to_pandas()


def read_from_parquet(cache_path):
    """Read from a deltatable into a pandas dataframe."""
    return dd.read_parquet(cache_path, engine='pyarrow')


def write_to_parquet(cache_path, df, overwrite=False):
    """Write a pandas or dask dataframe to a parquet."""
    part = None
    if hasattr(df, 'cache_partition'):
        part = df.cache_partition

    if isinstance(df, dd.DataFrame):
        df.to_parquet(cache_path, overwrite=overwrite, partition_on=part, engine='pyarrow')
    elif isinstance(df, pd.DataFrame):
        df.to_parquet(cache_path, partition_cols=part, engine='pyarrow')
    else:
        raise ValueError("Can only write dask and pandas dataframes to parquet.")


def write_to_delta(cache_path, df, overwrite=False):
    """Write a pandas dataframe to a delta table."""
    if isinstance(df, dd.DataFrame):
        print("""Warning: Dask datafame passed to delta backend. Will run `compute()`
                  on the dataframe prior to storage. This will fail if the dataframe
                  does not fit in memory. Use `backend=parquet` to handle parallel writing of dask dataframes.""")
        df = df.compute()

    if overwrite:
        write_deltalake(cache_path, df, mode='overwrite', schema_mode='overwrite')
    else:
        write_deltalake(cache_path, df)


def write_to_zarr(ds, cache_path, verify_path):
    """Write to zarr with a temp write and move to make it more atomic.

    # If you were to make this atomic this is what it would look like:
    lock = verify_path + ".lock"

    storage_client = storage.Client()
    blob = Blob.from_string(lock, client=storage_client)

    try:
        blob.upload_from_string("lock", if_generation_match=0)
    except google.api_core.exceptions.PreconditionFailed:
        raise RuntimeError(f"Concurrent zarr write detected. If this is a mistake delete the lock file: {lock}")

    fs.rm(lock)
    """
    fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')

    if fs.exists(verify_path):
        fs.rm(verify_path, recursive=True)

    if fs.exists(cache_path):
        fs.rm(cache_path, recursive=True)

    cache_map = fs.get_mapper(cache_path)
    ds.to_zarr(store=cache_map, mode='w')

    fs.touch(verify_path)


def chunk_to_zarr(ds, cache_path, verify_path, chunking):
    """Write a dataset to a zarr cache map and check the chunking."""
    ds = drop_encoded_chunks(ds)

    if isinstance(chunking, dict):
        # No need to prune if chunking is None or 'auto'
        chunking = prune_chunking_dimensions(ds, chunking)
    ds = ds.chunk(chunks=chunking)
    try:
        chunk_size, chunk_with_labels = get_chunk_size(ds)

        if chunk_size > CHUNK_SIZE_UPPER_LIMIT_MB or chunk_size < CHUNK_SIZE_LOWER_LIMIT_MB:
            print(f"WARNING: Chunk size is {chunk_size}MB. Target approx 100MB.")
            print(chunk_with_labels)
    except ValueError:
        print("Failed to get chunks size! Continuing with unknown chunking...")
    write_to_zarr(ds, cache_path, verify_path)


def postgres_table_name(table_name):
    """Return a qualified postgres table name."""
    return hashlib.md5(table_name.encode()).hexdigest()


def read_from_postgres(table_name, hash_table_name=True):
    """Read a pandas df from a table in the sheerwater postgres.

    Backends should eventually be flexibly specified, but for now
    we'll just hard code a connection to the running sheerwater database.

    Args:
        table_name (str): The table name to read from
        hash_table_name (bool): whether to hash the table name. Default true
    """
    # Get the postgres write secret
    pgread_pass = postgres_read_password()

    if hash_table_name:
        table_name = postgres_table_name(table_name)

    try:
        engine = sqlalchemy.create_engine(
            f'postgresql://read:{pgread_pass}@sheerwater-benchmarking-postgres:5432/postgres')
        df = pd.read_sql_query(f'select * from "{table_name}"', con=engine)
        return df
    except sqlalchemy.exc.InterfaceError:
        raise RuntimeError("""Error connecting to database. Make sure you are on the
                           tailnet and can see sheerwater-benchmarking-postgres.""")


def check_exists_postgres(table_name):
    """Check if table exists in postgres.

    Args:
        table_name: The table name to check
    """
    # Get the postgres write secret
    pgread_pass = postgres_read_password()

    table_name = postgres_table_name(table_name)

    try:
        engine = sqlalchemy.create_engine(
            f'postgresql://read:{pgread_pass}@sheerwater-benchmarking-postgres:5432/postgres')
        insp = sqlalchemy.inspect(engine)
        return insp.has_table(table_name)
    except sqlalchemy.exc.InterfaceError:
        raise RuntimeError("""Error connecting to database. Make sure you are on
                           the tailnet and can see sheerwater-benchmarking-postgres.""")


def write_to_postgres(pandas_df, table_name, overwrite=False):
    """Writes a pandas df as a table in the sheerwater postgres.

    Backends should eventually be flexibly specified, but for now
    we'll just hard code a connection to the running sheerwater database.

    Args:
        pandas_df: A pandas dataframe
        table_name: The table name to write to
        overwrite (bool): whether to overwrite an existing table
    """
    # Get the postgres write secret
    pgwrite_pass = postgres_write_password()

    new_table_name = postgres_table_name(table_name)

    try:
        engine = sqlalchemy.create_engine(
            f'postgresql://write:{pgwrite_pass}@sheerwater-benchmarking-postgres:5432/postgres')
        if overwrite:
            pandas_df.to_sql(new_table_name, engine, if_exists='replace')
        else:
            pandas_df.to_sql(new_table_name, engine)

        # Also log the table name in the tables table
        pd_name = {'table_name': [table_name], 'table_key': [new_table_name], 'created_at': [pd.Timestamp.now()]}
        pd_name = pd.DataFrame(pd_name)
        pd_name.to_sql('cache_tables', engine, if_exists='append')

    except sqlalchemy.exc.InterfaceError:
        raise RuntimeError("""Error connecting to database. Make sure you are on the tailnet
                           and can see sheerwater-benchmarking-postgres.""")


def write_to_terracotta(cache_key, ds):
    """Write geospatial array to terracotta.

    Args:
        cache_key(str): The unique key to store the data in terracotta
        ds (xr.Dataset): Dataset which holds raster
        clip_extreme_quantile(float): The quantile to clip the data at
    """
    # Check to make sure this is geospatial data
    lats = ['lat', 'y', 'latitude']
    lons = ['lon', 'x', 'longitude']
    if len(ds.dims) != 2:
        if len(ds.dims) != 3 or 'time' not in ds.dims:
            raise RuntimeError("Can only store two dimensional geospatial data to terracotta")

    foundx = False
    foundy = False
    for y in lats:
        if y in ds.dims:
            ds = ds.rename({y: 'y'})
            foundy = True
    for x in lons:
        if x in ds.dims:
            ds = ds.rename({x: 'x'})
            foundx = True

    if not foundx or not foundy:
        raise RuntimeError("Can only store two or three dimensional (with time) geospatial data to terracotta")

    # Adjust coordinates
    if (ds['x'] > 180.0).any():
        lon_base_change(ds, lon_dim='x')
        ds = ds.sortby(['x'])

    # Adapt the CRS
    ds.rio.write_crs("epsg:4326", inplace=True)
    ds = ds.rio.reproject('EPSG:3857', resampling=Resampling.nearest, nodata=np.nan)
    ds.rio.write_crs("epsg:3857", inplace=True)

    def write_individual_raster(driver, bucket, ds, cache_key):
        # Write the raster
        with MemoryFile() as mem_dst:
            ds.rio.to_raster(mem_dst.name, driver="COG")

            blob = bucket.blob(f'rasters/{cache_key}.tif')
            blob.upload_from_file(mem_dst)

            driver.insert({'key': cache_key.replace('/', '_')}, mem_dst,
                          override_path=f'/mnt/sheerwater-datalake/{cache_key}.tif', skip_metadata=False)

            print(f"Inserted {cache_key.replace('/', '_')} into the terracotta database.")

    storage_client = storage.Client()
    bucket = storage_client.bucket("sheerwater-datalake")

    # Register with terracotta
    tc.update_settings(SQL_USER="write", SQL_PASSWORD=postgres_write_password())
    if not hasattr(write_to_terracotta, 'driver'):
        driver = tc.get_driver("postgresql://sheerwater-benchmarking-postgres:5432/terracotta")
        write_to_terracotta.driver = driver
    else:
        driver = write_to_terracotta.driver

    try:
        driver.get_keys()
    except sqlalchemy.exc.DatabaseError:
        # Create a metastore
        print("Creating new terracotta metastore")
        driver.create(['key'])

    # Insert the parameters.
    with driver.connect():
        if 'time' in ds.dims:
            for t in ds.time:
                # Select just this time and squeeze the dimension
                sub_ds = ds.sel(time=t)
                sub_ds = sub_ds.reset_coords('time', drop=True)

                # add the time to the cache_key
                sub_cache_key = cache_key + '_' + str(t.values)

                write_individual_raster(driver, bucket, sub_ds, sub_cache_key)
        else:
            write_individual_raster(driver, bucket, ds, cache_key)


def cache_exists(backend, cache_path, verify_path=None):
    """Check if a cache exists generically."""
    if backend in ['zarr', 'delta', 'pickle', 'terracotta', 'parquet']:
        # Check to see if the cache exists for this key
        fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
        if backend == 'zarr':
            if not fs.exists(verify_path) and fs.exists(cache_path):
                print("Found cache, but it appears to be corrupted. Recomputing.")
                return False
            elif fs.exists(verify_path) and fs.exists(cache_path):
                return True
            else:
                return False
        else:
            return fs.exists(cache_path)
    elif backend == 'postgres':
        return check_exists_postgres(cache_path)
    else:
        raise ValueError(f'Unknown backend {backend}')


def update_config_from_kwargs(cache_config, passed_kwargs):
    """Extract the cache arguments from the kwargs and return them."""
    for k in cache_config:
        if k in passed_kwargs and passed_kwargs[k] is not None:
            # Overwrite the cache config parameter with the passed kwargs
            # if provided and is not None
            cache_config[k] = passed_kwargs[k]
            # Remove cache config parameter from the passed kwargs
            del passed_kwargs[k]
    return cache_config


def get_cache_config(cache_config, func, args, kwargs):
    """Get the runtime configuration of the cache.

    Args:
        cache_config(dict): The cache configuration
        func(function): The function to cache
        args(list): The positional arguments to the function
        kwargs(dict): The keyword arguments to the function

    Returns:
        dict: The runtime configuration of the cache
    """
    # A local copy to modify
    config = cache_config.copy()

    # Populate with the default cache params and runtime kwarg values
    config = update_config_from_kwargs(config, kwargs)

    # Get the parameters of the function
    params = signature(func).parameters

    # Validate time series params
    start_time = None
    end_time = None
    if config['timeseries'] is not None:
        # Convert to a list if not
        timeseries = config['timeseries'] if isinstance(config['timeseries'], list) else [config['timeseries']]

        if 'start_time' in config['cache_args'] or 'end_time' in config['cache_args']:
            raise ValueError(
                "Time series functions must not place their time arguments in cacheable_args!")

        if 'start_time' not in params or 'end_time' not in params:
            raise ValueError(
                "Time series functions must have the parameters 'start_time' and 'end_time'")
        else:
            keys = [item for item in params]
            try:
                start_time = args[keys.index('start_time')]
                end_time = args[keys.index('end_time')]
            except IndexError:
                raise ValueError("'start_time' and 'end_time' must be passed as positional arguments, not "
                                 "keyword arguments")

        # Add the start and end time to the cache config
        config['start_time'] = start_time
        config['end_time'] = end_time
        config['timeseries'] = timeseries

    # Process the list of cache arguments to get their values
    cache_arg_values = {}
    for a in config['cache_args']:
        # If it's in kwargs, great
        if a in kwargs:
            cache_arg_values[a] = kwargs[a]
            continue

        # If it's not in kwargs it must either be (1) in args or (2) passed as default
        found = False
        for i, p in enumerate(params):
            if (a == p and len(args) > i and
                    (params[p].kind == Parameter.VAR_POSITIONAL or
                        params[p].kind == Parameter.POSITIONAL_OR_KEYWORD)):
                cache_arg_values[a] = args[i]
                found = True
                break
            elif a == p and params[p].default != Parameter.empty:
                cache_arg_values[a] = params[p].default
                found = True
                break

        if not found:
            raise RuntimeError(f"Specified cacheable argument {a} "
                               "not discovered as passed argument or default argument.")

    # Now that we have cache arguments, update cache configuration (merge chunk by arg, cache disable)
    config['chunking'] = merge_chunk_by_arg(
        config['chunking'], config['chunk_by_arg'], cache_arg_values)

    # Now that we have all the cacheable args values we can calculate whether
    # the cache should be disable from them if
    if config['cache'] and \
            (isinstance(config['cache_disable_if'], dict) or isinstance(config['cache_disable_if'], list)):
        config['cache'] = check_cache_disable_if(config['cache_disable_if'], cache_arg_values)
    elif config['cache'] and config['cache_disable_if'] is not None:
        raise ValueError("cache_disable_if only accepts a dict or list of dicts.")

    # Form the cache key from the cache args and values
    imkeys = list(cache_arg_values.keys())
    imkeys.sort()
    sorted_values = [cache_arg_values[i] for i in imkeys]
    flat_values = []
    for val in sorted_values:
        if isinstance(val, list):
            sub_vals = [str(v) for v in val]
            sub_vals.sort()
            flat_values += sub_vals
        elif isinstance(val, dict):
            sub_vals = [f"{k}-{v}" for k, v in val.items()]
            sub_vals.sort()
            flat_values += sub_vals
        else:
            flat_values.append(str(val))

    cache_key = func.__name__ + '/' + '_'.join(flat_values)
    config['cache_key'] = cache_key

    # Set the backend-specific cache names
    if config['data_type'] == 'array':
        config['backend'] = "zarr" if config['backend'] is None else config['backend']
        if config['storage_backend'] is None:
            config['storage_backend'] = config['backend']

        config['cache_path'] = "gs://sheerwater-datalake/caches/" + cache_key + '.zarr'
        config['verify_path'] = "gs://sheerwater-datalake/caches/" + cache_key + '.verify'
        config['null_path'] = "gs://sheerwater-datalake/caches/" + cache_key + '.null'
        config['supports_filepath'] = True
    elif config['data_type'] == 'tabular':
        # Set the default backend to delta
        config['backend'] = "delta" if config['backend'] is None else config['backend']
        if config['storage_backend'] is None:
            config['storage_backend'] = config['backend']

        if config['backend'] == 'delta':
            config['cache_path'] = "gs://sheerwater-datalake/caches/" + cache_key + '.delta'
            config['verify_path'] = None
            config['null_path'] = "gs://sheerwater-datalake/caches/" + cache_key + '.null'
            config['supports_filepath'] = True
        elif config['backend'] == 'parquet':
            config['cache_path'] = "gs://sheerwater-datalake/caches/" + cache_key + '.parquet'
            config['verify_path'] = None
            config['null_path'] = "gs://sheerwater-datalake/caches/" + cache_key + '.null'
            config['supports_filepath'] = True
        elif config['backend'] == 'postgres':
            config['cache_path'] = cache_key
            config['verify_path'] = None
            config['null_path'] = "gs://sheerwater-datalake/caches/" + cache_key + '.null'
            config['supports_filepath'] = False
        else:
            raise ValueError("Only delta, parquet, and postgres backends are supported for tabular data")
    elif config['data_type'] == 'basic':
        config['backend'] = "pickle" if config['backend'] is None else config['backend']
        if config['storage_backend'] is None:
            config['storage_backend'] = config['backend']
        config['cache_path'] = "gs://sheerwater-datalake/caches/" + cache_key + '.pkl'
        config['verify_path'] = None
        config['null_path'] = "gs://sheerwater-datalake/caches/" + cache_key + '.null'
        config['supports_filepath'] = True
    else:
        raise ValueError("Caching currently only supports the 'array', 'tabular', and 'basic' datatypes")

    if config['filepath_only'] and not config['supports_filepath']:
        raise ValueError(f"{config['backend']} backend does not support filepath_only flag")

    return config


class cacheable:
    """Decorator for handling typed data loading, storing, and composable computation.

    Args:
        data_type(str): The type of data being cached. One of 'array', 'tabular', or 'basic'.
        cache_args(list): The arguments to use as the cache key.
        timeseries(str, list): The name of the time series dimension (for array data) or column (for
            tabular data) in the cached array. If not a time series, set to None (default). If a
            list, will use the first matching coordinate in the list.
        chunking(dict): Specifies chunking if that coordinate exists. If coordinate does not exist
            the chunking specified will be dropped.
        chunk_by_arg(dict): Specifies chunking modifiers based on the passed cached arguments,
            e.g. grid resolution.  For example:
            chunk_by_arg={
                'grid': {
                    'global0_25': {"lat": 721, "lon": 1440, 'time': 30}
                    'global1_5': {"lat": 121, "lon": 240, 'time': 1000}
                }
            }
            will modify the chunking dict values for lat, lon, and time, depending
            on the value of the 'grid' argument. If multiple cache arguments specify
            modifiers for the same chunking dimension, the last one specified will prevail.
        auto_rechunk(bool): If True will aggressively rechunk a cache on load.
        cache(bool): Whether to cache the result.
        validate_cache_timeseries(bool): Whether to validate the cache timeseries against the
            requested timeseries. If False, will not validate the cache timeseries.
        force_overwrite(bool): Whether to overwrite the cache if it
            already exists (if False, will prompt the user before overwriting).
        retry_null_cache(bool): If True, ignore and delete the null caches and attempts to recompute
            result for null values. If False (default), will return None for null caches.
        cache_disable_if(dict, list): If the cache arguments match the dict or list of dicts
            then the cache will be disabled. This is useful for disabling caching based on
            certain arguments. Defaults to None.
        backend(str): The name of the backend to use for cache recall/storage. None for
            default, zarr, delta, postgres, terracotta.
        storage_backend(str): The name of the backend to use for cache storage only. None
            to match backend. Useful for pulling from one backend and writing to another.
        auto_rechunk(bool): If True, will rechunk the cache on load if the cache chunking
            does not match the requested chunking. Default is False.
    """

    def __init__(self, data_type, cache_args,
                 timeseries=None, validate_cache_timeseries=True,
                 chunking=None, chunk_by_arg=None, auto_rechunk=False,
                 cache=True, recompute=False, force_overwrite=False, retry_null_cache=False,
                 cache_disable_if=None,
                 filepath_only=False,
                 backend=None, storage_backend=None):
        """Initialize cacheable filesystems and configuration."""
        # Initialize the google filesystem
        self.fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')

        self.config = {
            'data_type': data_type,
            'cache_args': cache_args,
            'timeseries': timeseries,
            'validate_cache_timeseries': validate_cache_timeseries,
            'chunking': chunking,
            'chunk_by_arg': chunk_by_arg,
            'auto_rechunk': auto_rechunk,
            'cache': cache,
            'recompute': recompute,
            'force_overwrite': force_overwrite,
            'retry_null_cache': retry_null_cache,
            'cache_disable_if': cache_disable_if,
            'filepath_only': filepath_only,
            'backend': backend,
            'storage_backend': storage_backend
        }

    def __call__(self, func):
        @wraps(func)
        def cacheable_wrapper(*args, **kwargs):
            # The config that determines cacheable behavior, based on default and passed arguments
            config = get_cache_config(self.config, func, args, kwargs)

            ds = None
            compute_result = True

            # Delete the null cache if retry null cache is passed
            if self.fs.exists(config['null_path']) and config['retry_null_cache']:
                print(f"Removing and retrying null cache {config['null_path']}.")
                self.fs.rm(config['null_path'], recursive=True)

            if not config['recompute'] and config['cache']:
                if cache_exists(config['backend'], config['cache_path'], config['verify_path']):
                    # Read the cache
                    print(f"Found cache for {config['cache_path']}")
                    if config['data_type'] == 'array':
                        cache_map = self.fs.get_mapper(config['cache_path'])
                        if config['filepath_only']:
                            return cache_map
                        else:
                            print(f"Opening cache {config['cache_path']}")
                            # We must auto open chunks. This tries to use the underlying zarr chunking if possible.
                            # Setting chunks=True triggers what I think is an xarray/zarr engine bug where
                            # every chunk is only 4B!
                            ds = xr.open_dataset(cache_map, engine='zarr', chunks={}, decode_timedelta=True)

                            # If rechunk is passed then check to see if the rechunk array
                            # matches chunking. If not then rechunk
                            if config['auto_rechunk']:
                                if not isinstance(config['chunking'], dict):
                                    raise ValueError(
                                        "If auto_rechunk is True, a chunking dict must be supplied.")

                                # Compare the dict to the rechunk dict
                                if not chunking_compare(ds, config['chunking']):
                                    print(
                                        "Rechunk was passed and cached chunks do not match rechunk request. "
                                        "Performing rechunking.")

                                    # write to a temp cache map
                                    # writing to temp cache is necessary because if you overwrite
                                    # the original cache map it will write it before reading the
                                    # data leading to corruption.
                                    temp_cache_path = 'gs://sheerwater-datalake/caches/temp/' + \
                                        config['cache_key'] + '.temp'
                                    temp_verify_path = 'gs://sheerwater-datalake/caches/temp/' + \
                                        config['cache_key'] + '.verify'
                                    chunk_to_zarr(ds, temp_cache_path, temp_verify_path, config['chunking'])

                                    if self.fs.exists(config['verify_path']):
                                        self.fs.rm(config['verify_path'], recursive=True)

                                    if self.fs.exists(config['cache_path']):
                                        self.fs.rm(config['cache_path'], recursive=True)

                                    self.fs.mv(temp_cache_path, config['cache_path'], recursive=True)
                                    self.fs.mv(temp_verify_path, config['verify_path'], recursive=True)

                                    # Reopen the dataset
                                    ds = xr.open_dataset(cache_map, engine='zarr', chunks={}, decode_timedelta=True)
                                else:
                                    # Requested chunks already match rechunk.
                                    pass

                            if config['validate_cache_timeseries'] and config['timeseries'] is not None:
                                # Check to see if the dataset extends roughly the full time series set
                                match_time = [t for t in config['timeseries'] if t in ds.dims]
                                if len(match_time) == 0:
                                    raise RuntimeError("Timeseries array functions must return "
                                                       "a time dimension for slicing. "
                                                       "This could be an invalid cache. "
                                                       "Try running with recompute=True to reset the cache.")
                                else:
                                    time_col = match_time[0]

                                    # Assign start and end times if None are passed
                                    st = dateparser.parse(config['start_time']) if config['start_time'] is not None \
                                        else pd.Timestamp(ds[time_col].min().values)
                                    et = dateparser.parse(config['end_time']) if config['end_time'] is not None \
                                        else pd.Timestamp(ds[time_col].max().values)

                                    # Check if within 1 year at least
                                    if (pd.Timestamp(ds[time_col].min().values) <
                                        st + datetime.timedelta(days=365) and
                                            pd.Timestamp(ds[time_col].max().values) >
                                            et - datetime.timedelta(days=365)):
                                        compute_result = False
                                    else:
                                        print("WARNING: The cached array does not have data within "
                                              "1 year of your start or end time. Automatically recomputing. "
                                              "If you do not want to recompute the result set "
                                              "`validate_cache_timeseries=False`")
                            else:
                                compute_result = False
                    elif config['data_type'] == 'tabular':
                        if config['backend'] == 'delta':
                            if config['filepath_only']:
                                cache_map = self.fs.get_mapper(config['cache_path'])
                                return cache_map
                            else:
                                print(f"Opening cache {config['cache_path']}")
                                ds = read_from_delta(config['cache_path'])

                                if config['validate_cache_timeseries'] and config['timeseries'] is not None:
                                    raise NotImplementedError("""Timeseries validation is not currently implemented
                                                              for tabular datasets.""")
                                else:
                                    compute_result = False
                        elif config['backend'] == 'parquet':
                            if config['filepath_only']:
                                return config['cache_path']
                            else:
                                print(f"Opening cache {config['cache_path']}")
                                ds = read_from_parquet(config['cache_path'])

                                if config['validate_cache_timeseries'] and config['timeseries'] is not None:
                                    raise NotImplementedError("""Timeseries validation is not currently implemented
                                                              for tabular datasets.""")
                                else:
                                    compute_result = False

                        elif config['backend'] == 'postgres':
                            ds = read_from_postgres(config['cache_key'])
                            if config['validate_cache_timeseries'] and config['timeseries'] is not None:
                                raise NotImplementedError("""Timeseries validation is not currently implemented
                                                          for tabular datasets""")
                            else:
                                compute_result = False
                        else:
                            raise ValueError("""Only delta, parquet, and postgres backends are
                                             supported for tabular data.""")
                    elif config['data_type'] == 'basic':
                        if config['backend'] == 'pickle':
                            if config['filepath_only']:
                                cache_map = self.fs.get_mapper(config['cache_path'])
                                return cache_map
                            else:
                                print(f"Opening cache {config['cache_path']}")
                                with self.fs.open(config['cache_path']) as f:
                                    ds = pickle.load(f)

                                if config['validate_cache_timeseries'] and config['timeseries'] is not None:
                                    raise NotImplementedError("""Timeseries validation is not currently implemented
                                                              for basic datasets.""")
                                else:
                                    compute_result = False
                        else:
                            raise ValueError("Only pickle backend supported for basic types.")
                    else:
                        print("Auto caching currently only supports array, tabular, and basic types.")
                elif self.fs.exists(config['null_path']) and not config['recompute'] and config['cache'] and not config['retry_null_cache']:
                    print(f"Found null cache for {config['null_path']}. Skipping computation.")
                    return None

            if compute_result or config['backend'] != config['storage_backend']:
                if compute_result:
                    if config['recompute']:
                        print(f"Recompute for {config['cache_key']} requested. Not checking for cached result.")
                    elif not config['cache']:
                        # The function isn't cacheable, recomputing
                        pass
                    else:
                        print(f"Cache doesn't exist for {config['cache_key']}. Running function")

                    if config['timeseries'] is not None and (config['start_time'] is None or config['end_time'] is None):
                        raise ValueError('Need to pass start and end time arguments when recomputing function.')

                    ##### IF NOT EXISTS ######
                    ds = func(*args, **kwargs)
                    ##########################

                # Store the result
                if config['cache']:
                    if ds is None:
                        print(f"Autocaching null result for {config['null_path']}.")
                        with self.fs.open(config['null_path'], 'wb') as f:
                            f.write(b'')
                            return None

                    write = False  # boolean to determine if we should write to the cache
                    if config['data_type'] == 'array':
                        if config['storage_backend'] == 'zarr':
                            if cache_exists(config['storage_backend'], config['cache_path'], config['verify_path']) and not config['force_overwrite']:
                                inp = input(f'A cache already exists at {config["cache_path"]}.'
                                            'Are you sure you want to overwrite it? (y/n)')
                                if inp == 'y' or inp == 'Y':
                                    write = True
                            else:
                                write = True

                            if write:
                                print(f"Caching result for {config['cache_path']} as zarr.")
                                if isinstance(ds, xr.Dataset):
                                    cache_map = self.fs.get_mapper(config['cache_path'])
                                    chunk_config = config['chunking'] if config['chunking'] else 'auto'
                                    chunk_to_zarr(ds, config['cache_path'], config['verify_path'], chunk_config)
                                    # Reopen the dataset to truncate the computational path
                                    ds = xr.open_dataset(cache_map, engine='zarr', chunks={}, decode_timedelta=True)
                                else:
                                    raise RuntimeError(
                                        f"Array datatypes must return xarray datasets or None instead of {type(ds)}")
                        # Either way if terracotta is specified as a backend try to write the result array to terracotta
                        elif config['storage_backend'] == 'terracotta':
                            print(f"Also caching {config['cache_key']} to terracotta.")
                            write_to_terracotta(config['cache_key'], ds)
                    elif config['data_type'] == 'tabular':
                        if not (isinstance(ds, pd.DataFrame) or isinstance(ds, dd.DataFrame)):
                            raise RuntimeError(f"""Tabular datatypes must return pandas or dask dataframe
                                               or none instead of {type(ds)}""")

                        # TODO: combine repeated code
                        if config['storage_backend'] == 'delta':
                            if self.fs.exists(config['cache_path']) and not config['force_overwrite']:
                                inp = input(f'A cache already exists at {config["cache_path"]}.'
                                            'Are you sure you want to overwrite it? (y/n)')
                                if inp == 'y' or inp == 'Y':
                                    write = True
                            else:
                                write = True

                            if write:
                                print(f"Caching result for {config['cache_path']} in delta.")
                                write_to_delta(config['cache_path'], ds, overwrite=True)
                                # Reopen dataset to truncate the computational path
                                ds = read_from_delta(config['cache_path'])
                        elif config['storage_backend'] == 'parquet':
                            if self.fs.exists(config['cache_path']) and not config['force_overwrite']:
                                inp = input(f'A cache already exists at {config["cache_path"]}.'
                                            'Are you sure you want to overwrite it? (y/n)')
                                if inp == 'y' or inp == 'Y':
                                    write = True
                            else:
                                write = True

                            if write:
                                print(f"Caching result for {config['cache_path']} in parquet.")
                                write_to_parquet(config['cache_path'], ds, overwrite=True)
                                # Reopen dataset to truncate the computational path
                                ds = read_from_parquet(config['cache_path'])

                        elif config['storage_backend'] == 'postgres':
                            if check_exists_postgres(config['cache_key']) and not config['force_overwrite']:
                                inp = input(f'A cache already exists at {config["cache_path"]}.'
                                            'Are you sure you want to overwrite it? (y/n)')
                                if inp == 'y' or inp == 'Y':
                                    write = True
                            else:
                                write = True

                            if write:
                                print(f"Caching result for {config['cache_key']} in postgres.")
                                write_to_postgres(ds, config['cache_key'], overwrite=True)
                                # Reopen dataset to truncate the computational path
                                ds = read_from_postgres(config['cache_key'])
                        else:
                            raise ValueError("Only delta and postgres backends are implemented for tabular data")
                    elif config['data_type'] == 'basic':
                        if config['backend'] == 'pickle':
                            if self.fs.exists(config['cache_path']) and not config['force_overwrite']:
                                inp = input(f'A cache already exists at {config["cache_path"]}.'
                                            'Are you sure you want to overwrite it? (y/n)')
                                if inp == 'y' or inp == 'Y':
                                    write = True
                            else:
                                write = True

                            if write:
                                print(f"Caching result for {config['cache_path']}.")
                                with self.fs.open(config['cache_path'], 'wb') as f:
                                    pickle.dump(ds, f)
                        else:
                            raise ValueError("Only pickle backend is implemented for basic data data")

            if config['filepath_only']:
                if config['backend'] == 'parquet':
                    return config['cache_path']
                else:
                    cache_map = self.fs.get_mapper(config['cache_path'])
                    return cache_map
            else:
                # Do the time series filtering
                if config['timeseries'] is not None:
                    if config['data_type'] == "array":
                        match_time = [t for t in config['timeseries'] if t in ds.dims]
                    elif config['data_type'] == "tabular":
                        match_time = [t for t in config['timeseries'] if t in ds.columns]
                    else:
                        raise ValueError(
                            f"Timeseries is only supported for array or tabular data, not {config['data_type']}"
                        )

                    if len(match_time) == 0:
                        raise RuntimeError(
                            f"Timeseries must have a dimension or column named {config['timeseries']} for slicing."
                        )

                    time_col = match_time[0]

                    # Assign start and end times if None are passed
                    start_time = config['start_time'] if config['start_time'] is not None else ds[time_col].min().values
                    end_time = config['end_time'] if config['end_time'] is not None else ds[time_col].max().values
                    if config['data_type'] == 'array' and isinstance(ds, xr.Dataset):
                        ds = ds.sel({time_col: slice(start_time, end_time)})
                    elif config['data_type'] == 'tabular' and (isinstance(ds, pd.DataFrame) or isinstance(ds, dd.DataFrame)):
                        ds = ds[(ds[time_col] >= start_time) & (ds[time_col] <= end_time)]

                return ds
        return cacheable_wrapper
