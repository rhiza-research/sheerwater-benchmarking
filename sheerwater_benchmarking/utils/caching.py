"""Automated dataframe caching utilities."""
import os
import inspect
import datetime
import dateparser
from functools import wraps
from inspect import signature, Parameter
import logging
import hashlib
import fsspec
import uuid

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

# Backends that support local caching
SUPPORTS_LOCAL_CACHING = ['zarr', 'parquet', 'pickle']
SUPPORTS_VERIFY_FILE = ['zarr', 'parquet', 'pickle']

# Global variables for caching configuration
global_recompute = None
global_force_overwrite = None
global_validate_cache_timeseries = None
global_retry_null_cache = None

# Remote dir for all caches, except for postgres and terracotta
CACHE_ROOT_DIR = "gs://sheerwater-datalake/caches/"
CACHE_STORAGE_OPTIONS = {
    'project': 'sheerwater',
    'token': 'google_default'
}
# Local dir for all caches, except for postgres and terracotta
LOCAL_CACHE_ROOT_DIR = os.path.expanduser("~/.cache/sheerwater/caches/")
LOCAL_CACHE_STORAGE_OPTIONS = {}

# Check if these are defined as environment variables
if 'SHEERWATER_CACHE_ROOT_DIR' in os.environ:
    CACHE_ROOT_DIR = os.environ['SHEERWATER_CACHE_ROOT_DIR']
if 'SHEERWATER_LOCAL_CACHE_ROOT_DIR' in os.environ:
    LOCAL_CACHE_ROOT_DIR = os.environ['SHEERWATER_LOCAL_CACHE_ROOT_DIR']


def set_global_cache_variables(recompute=None, force_overwrite=None,
                               validate_cache_timeseries=None, retry_null_cache=None):
    """Reset all global variables to defaults and set the new values."""
    global global_recompute, global_force_overwrite, \
        global_validate_cache_timeseries, global_retry_null_cache

    # Simple logic for global variables
    global_force_overwrite = force_overwrite
    global_validate_cache_timeseries = validate_cache_timeseries
    global_retry_null_cache = retry_null_cache

    # More complex logic for recompute
    if recompute == True:  # noqa: E712
        global_recompute = False
    elif isinstance(recompute, str) and recompute != '_all':
        # if a single function's name is passed, convert to a list
        global_recompute = [recompute]
    else:
        global_recompute = recompute  # if recompute is false, '_all' or a list


def check_if_nested_fn():
    """Check if the current scope downstream from another cached function."""
    # Get the current frame
    stack = inspect.stack()
    # skip the first two frames (this function and the current cacheable function)
    for frame_info in stack[2:]:
        frame = frame_info.frame
        func_name = frame.f_code.co_name
        if 'wrapper' in func_name:
            # There is a cachable function upstream of this one
            return True
    # No cachable function upstream of this one
    return False


def get_local_cache(cache_path):
    """Get the local cache path based on the file system.

    Args:
        cache_path (str): Path to cache file

    Returns:
        str: Local cache path
    """
    if cache_path is None:
        return None
    if not cache_path.startswith(CACHE_ROOT_DIR):
        raise ValueError("Cache path must start with CACHE_ROOT_DIR")

    cache_key = cache_path.split(CACHE_ROOT_DIR)[1]
    return os.path.join(LOCAL_CACHE_ROOT_DIR, cache_key)


def sync_local_remote(backend, cache_fs, local_fs,
                      cache_path=None, local_cache_path=None,
                      verify_path=None, null_path=None):
    """Sync a local cache mirror to a remote cache.

    Args:
        backend (str): The backend to use for the cache
        cache_fs (fsspec.core.url_to_fs): The filesystem of the cache
        local_fs (fsspec.core.url_to_fs): The filesystem of the local mirror
        cache_path (str): The path to the cache
        local_cache_path (str): The path to the local cache
        verify_path (str): The path to the verify file
        null_path (str): The path to the null file
    """
    if local_cache_path == cache_path:
        return  # don't sync if the read and the remote filesystems are the same

    # Syncing is only possible if the remote cache exists and is valid
    assert backend in SUPPORTS_LOCAL_CACHING
    assert cache_exists(backend, cache_path, verify_path)

    local_verify_path = get_local_cache(verify_path)
    local_null_path = get_local_cache(null_path)

    # Remove the local null cache if it doesn't exist on remote
    if not cache_fs.exists(null_path) and local_fs.exists(local_null_path):
        local_fs.rm(local_null_path, recursive=True)

    # If the cache exists locally and is valid, we're synced - return!
    if cache_exists(backend, local_cache_path, local_verify_path):
        # If we have a local cache, check and make sure the verify timestamp is the same
        local_verify_ts = local_fs.open(local_verify_path, 'r').read()
        remote_verify_ts = cache_fs.open(verify_path, 'r').read()
        if local_verify_ts == remote_verify_ts:
            return

    # If the verify timestamp is different, or it doesn't exist, delete the local cache
    # and sync the remote cache to local
    if cache_path and cache_fs.exists(cache_path):
        if local_fs.exists(local_cache_path):
            local_fs.rm(local_cache_path, recursive=True)
        cache_fs.get(cache_path, local_cache_path, recursive=True)
    if verify_path and cache_fs.exists(verify_path):
        if local_fs.exists(local_verify_path):
            local_fs.rm(local_verify_path)
        cache_fs.get(verify_path, local_verify_path)
    if null_path and cache_fs.exists(null_path):
        if local_fs.exists(local_null_path):
            local_fs.rm(local_null_path)
        cache_fs.get(null_path, local_null_path)


def get_cache_args(kwargs, cache_kwargs):
    """Extract the cache arguments from the kwargs and return them."""
    cache_args = []
    for k in cache_kwargs:
        if k in kwargs:
            cache_args.append(kwargs[k])
            del kwargs[k]
        else:
            cache_args.append(cache_kwargs[k])
    return cache_args


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
    return dd.read_parquet(cache_path, engine='pyarrow', ignore_metadata_file=True)


def write_to_parquet(df, cache_path, verify_path, overwrite=False, upsert=False, primary_keys=None):
    """Write a pandas or dask dataframe to a parquet."""
    part = None
    if hasattr(df, 'cache_partition'):
        part = df.cache_partition

    fs = fsspec.core.url_to_fs(cache_path, **CACHE_STORAGE_OPTIONS)[0]


    if upsert and cache_exists('parquet', cache_path, verify_path):
        print("Found existing cache for upsert.")

        if fs.exists(verify_path):
            fs.rm(verify_path)

        if primary_keys is None:
            raise ValueError("Upsert may only be performed with primary keys specified")

        if not isinstance(df, dd.DataFrame):
            raise RuntimeError("Upsert is only supported by dask dataframes for parquet")

        existing_df = read_from_parquet(cache_path)

        # Record starting partitions
        start_parts = df.npartitions

        # remove any rows already in the dataframe
        outer_join = existing_df.merge(df, how = 'outer', indicator = True)
        new_rows = outer_join[(outer_join._merge == 'right_only')].drop('_merge', axis = 1)

        if len(new_rows.index) > 0:
            new_rows = new_rows.repartition(npartitions=start_parts)

            # Coearce dtypes - may not be necessary?
            # new_rows = new_rows.astype(existing_df.dtypes)

            # write in append mode
            print("Appending new rows to existing parquet.")
            new_rows.to_parquet(
                cache_path,
                overwrite=False,
                append=True,
                partition_on=part,
                engine="pyarrow",
                write_metadata_file=True,
                write_index=False,
            )
        else:
            print("No rows to upsert.")
    else:
        if fs.exists(cache_path):
            fs.rm(cache_path, recursive=True)

        if fs.exists(verify_path):
            fs.rm(verify_path)

        if isinstance(df, dd.DataFrame):
            df.to_parquet(
                cache_path,
                overwrite=overwrite,
                partition_on=part,
                engine="pyarrow",
                write_metadata_file=True,
                write_index=False,
            )
        elif isinstance(df, pd.DataFrame):
            df.to_parquet(cache_path, partition_cols=part, engine='pyarrow')
        else:
            raise ValueError("Can only write dask and pandas dataframes to parquet.")

    fs.open(verify_path, 'w').write(datetime.datetime.now(datetime.timezone.utc).isoformat())


def write_to_delta(df, cache_path, overwrite=False):
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
    fs = fsspec.core.url_to_fs(cache_path, **CACHE_STORAGE_OPTIONS)[0]
    if fs.exists(verify_path):
        fs.rm(verify_path, recursive=True)

    if fs.exists(cache_path):
        fs.rm(cache_path, recursive=True)

    cache_map = fs.get_mapper(cache_path)
    ds.to_zarr(store=cache_map, mode='w')

    # Add a lock file to the cache to verify cache integrity, with the current timestamp
    fs.open(verify_path, 'w').write(datetime.datetime.now(datetime.timezone.utc).isoformat())


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
        hash_table_name (bool): whether not to hash the table name. Default False.
    """
    # Get the postgres write secret
    pgread_pass = postgres_read_password()

    if hash_table_name:
        table_name = postgres_table_name(table_name)

    try:
        uri = f'postgresql://read:{pgread_pass}@sheerwater-benchmarking-postgres:5432/postgres'
        engine = sqlalchemy.create_engine(uri)

        df = pd.read_sql_query(f'select * from "{table_name}"', con=engine)
        return df

    except sqlalchemy.exc.InterfaceError:
        raise RuntimeError("""Error connecting to database. Make sure you are on the
                           tailnet and can see sheerwater-benchmarking-postgres.""")


def check_exists_postgres(table_name, hash_table_name=True):
    """Check if table exists in postgres.

    Args:
        table_name: The table name to check
        hash_table_name (bool): whether to hash the table name. Default False
    """
    # Get the postgres write secret
    pgread_pass = postgres_read_password()

    if hash_table_name:
        table_name = postgres_table_name(table_name)

    try:
        engine = sqlalchemy.create_engine(
            f'postgresql://read:{pgread_pass}@sheerwater-benchmarking-postgres:5432/postgres')
        insp = sqlalchemy.inspect(engine)
        return insp.has_table(table_name)
    except sqlalchemy.exc.InterfaceError:
        raise RuntimeError("""Error connecting to database. Make sure you are on
                           the tailnet and can see sheerwater-benchmarking-postgres.""")


def write_to_postgres(df, table_name, overwrite=False, upsert=False, primary_keys=None, hash_table_name=True):
    """Writes a pandas df as a table in the sheerwater postgres.

    Backends should eventually be flexibly specified, but for now
    we'll just hard code a connection to the running sheerwater database.

    Args:
        df: A pandas dataframe
        table_name: The table name to write to
        overwrite (bool): whether to overwrite an existing table
        upsert (bool): whether to upsert into table
        primary_keys (list(str)): column names of primary keys for upsert
        hash_table_name (bool): whether to hash the table name. Default false
    """
    # Get the postgres write secret
    pgwrite_pass = postgres_write_password()

    if not hash_table_name:
        new_table_name = table_name
    else:
        new_table_name = postgres_table_name(table_name)

    uri = f'postgresql://write:{pgwrite_pass}@34.59.163.82:5432/postgres'
    engine = sqlalchemy.create_engine(uri)


    if upsert and check_exists_postgres(table_name, hash_table_name=hash_table_name):
        if primary_keys is None or not isinstance(primary_keys, list):
            raise ValueError("Upsert may only be performed with primary keys specified as a list.")

        if not isinstance(df, dd.DataFrame):
            raise RuntimeError("Upsert is only supported by dask dataframes for parquet")

        with engine.begin() as conn:
            print("Postgres cache exists for upsert.")
            # If it already exists...

            # Extract the primary key columns for SQL constraint
            for key in primary_keys:
                if key not in df.columns:
                    raise ValueError("Dataframe MUST contain all primary keys as columns")

            # Write a temporary table
            temp_table_name = f"temp_{uuid.uuid4().hex[:6]}"

            if isinstance(df, pd.DataFrame):
                df.to_sql(temp_table_name, engine, index=False)
            elif isinstance(df, dd.DataFrame):
                df.to_sql(temp_table_name, uri=uri, index=False, parallel=True, chunksize=10000)
            else:
                raise RuntimeError("Did not return dataframe type.")

            index_sql_txt = ", ".join([f'"{i}"' for i in primary_keys])
            columns = list(df.columns)
            headers = primary_keys + list(set(columns) - set(primary_keys))
            headers_sql_txt = ", ".join(
                [f'"{i}"' for i in headers]
            )  # index1, index2, ..., column 1, col2, ...

            # col1 = exluded.col1, col2=excluded.col2
            # Excluded statement updates values of rows where primary keys conflict
            update_column_stmt = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns])

            # For the ON CONFLICT clause, postgres requires that the columns have unique constraint
            # To add if not exists must drop if exists and then add. In a transaction this is consistent
            query_pk = f"""
            ALTER TABLE "{new_table_name}" DROP CONSTRAINT IF EXISTS unique_constraint_for_upsert;
            ALTER TABLE "{new_table_name}" ADD CONSTRAINT unique_constraint_for_upsert UNIQUE ({index_sql_txt});
            """
            print("Adding a unique to contraint to table if it doesn't exist.")
            conn.exec_driver_sql(query_pk)

            # Compose and execute upsert query
            query_upsert = f"""INSERT INTO "{new_table_name}" ({headers_sql_txt})
                               SELECT {headers_sql_txt} FROM "{temp_table_name}"
                               ON CONFLICT ({index_sql_txt}) DO UPDATE
                               SET {update_column_stmt};
                               """
            print("Upserting.")
            conn.exec_driver_sql(query_upsert)
            conn.exec_driver_sql(f"DROP TABLE {temp_table_name}")
    else:
        try:
            exists = 'fail'
            if overwrite:
                exists = 'replace'

            if isinstance(df, pd.DataFrame):
                df.to_sql(new_table_name, engine, if_exists=exists, index=False)
            elif isinstance(df, dd.DataFrame):
                df.to_sql(new_table_name, uri=uri, if_exists=exists, index=False, parallel=True, chunksize=10000)
            else:
                raise RuntimeError("Did not return dataframe type.")

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

def cache_exists(backend, cache_path, verify_path=None, hash_postgres_table_name=True):
    """Check if a cache exists generically."""
    if backend in ['zarr', 'delta', 'pickle', 'terracotta', 'parquet']:
        # Check to see if the cache exists for this key
        fs = fsspec.core.url_to_fs(cache_path, **CACHE_STORAGE_OPTIONS)[0]
        if backend in SUPPORTS_VERIFY_FILE:
            if not fs.exists(verify_path) and fs.exists(cache_path):
                print("Found cache, but it appears to be corrupted.")
                return False
            elif fs.exists(verify_path) and fs.exists(cache_path):
                return True
            else:
                return False
        else:
            return fs.exists(cache_path)
    elif backend == 'postgres':
        return check_exists_postgres(cache_path, hash_table_name=hash_postgres_table_name)
    else:
        raise ValueError(f'Unknown backend {backend}')


def cacheable(data_type, cache_args, timeseries=None, chunking=None, chunk_by_arg=None,
              auto_rechunk=False, cache=True, validate_cache_timeseries=False, cache_disable_if=None,
              backend=None, storage_backend=None, cache_local=False,
              primary_keys=None, hash_postgres_table_name=True):
    """Decorator for caching function results.

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
        cache_local (bool): If True, will mirror the result locally, at the location
            specified by the LOCAL_CACHE_ROOT_DIR variable. Default is False.
        primary_keys (list(str)): Column names of the primary keys to user for upsert.
        hash_postgres_table_name (bool): Use the real table name for postgres writes.
    """
    # Valid configuration kwargs for the cacheable decorator
    cache_kwargs = {
        "filepath_only": False,
        "recompute": False,
        "cache": None,
        "validate_cache_timeseries": None,
        "force_overwrite": None,
        "retry_null_cache": False,
        "backend": None,
        "storage_backend": None,
        "auto_rechunk":  None,
        "cache_local": False,
        "upsert": False,
        "hash_postgres_table_name": True,
        "fail_if_no_cache": False,
    }

    nonlocals = locals()

    def create_cacheable(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Proper variable scope for the decorator args
            data_type = nonlocals['data_type']
            cache_args = nonlocals['cache_args']
            timeseries = nonlocals['timeseries']
            chunking = nonlocals['chunking']
            chunk_by_arg = nonlocals['chunk_by_arg']
            auto_rechunk = nonlocals['auto_rechunk']
            cache = nonlocals['cache']
            validate_cache_timeseries = nonlocals['validate_cache_timeseries']
            cache_disable_if = nonlocals['cache_disable_if']
            backend = nonlocals['backend']
            storage_backend = nonlocals['storage_backend']
            cache_local = nonlocals['cache_local']
            primary_keys = nonlocals['primary_keys']
            hash_postgres_table_name = nonlocals['hash_postgres_table_name']

            # Calculate the appropriate cache key
            filepath_only, recompute, passed_cache, passed_validate_cache_timeseries, \
                force_overwrite, retry_null_cache, passed_backend, \
                storage_backend, passed_auto_rechunk, passed_cache_local, \
                upsert, passed_hash_postgres_table_name, \
                fail_if_no_cache = get_cache_args(kwargs, cache_kwargs)

            if passed_cache is not None:
                cache = passed_cache
            if passed_auto_rechunk is not None:
                auto_rechunk = passed_auto_rechunk
            if passed_validate_cache_timeseries is not None:
                validate_cache_timeseries = passed_validate_cache_timeseries
            if passed_backend is not None:
                backend = passed_backend
            if passed_cache_local is not None:
                cache_local = passed_cache_local
            if passed_hash_postgres_table_name is not None:
                hash_postgres_table_name = passed_hash_postgres_table_name

            # Check if this is a nested cacheable function
            if not check_if_nested_fn():
                # This is a top level cacheable function, reset global cache variables
                set_global_cache_variables(recompute=recompute,
                                           force_overwrite=force_overwrite,
                                           validate_cache_timeseries=validate_cache_timeseries,
                                           retry_null_cache=retry_null_cache)
                if isinstance(recompute, list) or isinstance(recompute, str) or recompute == '_all':
                    recompute = True
            else:
                # Inherit global cache variables
                global global_recompute, global_force_overwrite, \
                    global_validate_cache_timeseries, global_retry_null_cache

                # Set all global variables
                if global_force_overwrite is not None:
                    force_overwrite = global_force_overwrite
                if global_validate_cache_timeseries is not None:
                    validate_cache_timeseries = global_validate_cache_timeseries
                if global_retry_null_cache is not None:
                    retry_null_cache = global_retry_null_cache
                if global_recompute:
                    if func.__name__ in global_recompute or global_recompute == '_all':
                        recompute = True

            params = signature(func).parameters

            # Validate time series params
            start_time = None
            end_time = None
            if timeseries is not None:
                # Convert to a list if not
                tl = timeseries if isinstance(timeseries, list) else [timeseries]

                if 'start_time' in cache_args or 'end_time' in cache_args:
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

            # Handle keying based on cache arguments
            cache_arg_values = {}

            for a in cache_args:
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

            # Update chunking based on chunk_by_arg
            chunking = merge_chunk_by_arg(chunking, chunk_by_arg, cache_arg_values)

            # Now that we have all the cacheable args values we can calculate whether
            # the cache should be disable from them if
            if cache and isinstance(cache_disable_if, dict) or isinstance(cache_disable_if, list):
                cache = check_cache_disable_if(cache_disable_if, cache_arg_values)
            elif cache and cache_disable_if is not None:
                raise ValueError("cache_disable_if only accepts a dict or list of dicts.")

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
            verify_path = None
            if data_type == 'array':
                backend = "zarr" if backend is None else backend
                if storage_backend is None:
                    storage_backend = backend

                cache_path = CACHE_ROOT_DIR + cache_key + '.zarr'
                verify_path = CACHE_ROOT_DIR + cache_key + '.verify'
                null_path = CACHE_ROOT_DIR + cache_key + '.null'
                supports_filepath = True
            elif data_type == 'tabular':
                # Set the default
                backend = "delta" if backend is None else backend
                if storage_backend is None:
                    storage_backend = backend

                if backend == 'delta':
                    cache_path = CACHE_ROOT_DIR + cache_key + '.delta'
                    null_path = CACHE_ROOT_DIR + cache_key + '.null'
                    supports_filepath = True
                elif backend == 'parquet':
                    cache_path = CACHE_ROOT_DIR + cache_key + '.parquet'
                    verify_path = CACHE_ROOT_DIR + cache_key + '.verify'
                    null_path = CACHE_ROOT_DIR + cache_key + '.null'
                    supports_filepath = True
                elif backend == 'postgres':
                    cache_path = cache_key
                    null_path = CACHE_ROOT_DIR + cache_key + '.null'
                    supports_filepath = False
                else:
                    raise ValueError("Only delta, parquet, and postgres backends are supported for tabular data")
            elif data_type == 'basic':
                backend = "pickle" if backend is None else backend
                if backend != 'pickle':
                    raise ValueError("Only pickle backend supported for basic types.")
                if storage_backend is None:
                    storage_backend = backend
                cache_path = CACHE_ROOT_DIR + cache_key + '.pkl'
                verify_path = CACHE_ROOT_DIR + cache_key + '.verify'
                null_path = CACHE_ROOT_DIR + cache_key + '.null'
                supports_filepath = True
            else:
                raise ValueError("Caching currently only supports the 'array', 'tabular', and 'basic' datatypes")

            if filepath_only and not supports_filepath:
                raise ValueError(f"{backend} backend does not support filepath_only flag")

            # Set up cached computation
            ds = None
            compute_result = True
            fs = fsspec.core.url_to_fs(cache_path, **CACHE_STORAGE_OPTIONS)[0]
            cache_map = fs.get_mapper(cache_path)

            # First remove null caches if retry null cache is passed
            if retry_null_cache and fs.exists(null_path):
                print(f"Removing and retrying null cache {null_path}.")
                fs.rm(null_path, recursive=True)

            read_cache_path = cache_path
            read_cache_map = cache_map
            read_fs = fs

            # If we are using local caching, and the cache exists, then we can use the local cache
            if cache_local and backend in SUPPORTS_LOCAL_CACHING:
                # If local, active cache can differ from the cache path
                read_cache_path = get_local_cache(cache_path)
                read_cache_map = fs.get_mapper(read_cache_path)
                read_fs = fsspec.core.url_to_fs(read_cache_path, **LOCAL_CACHE_STORAGE_OPTIONS)[0]

            # Now check if the cache exists
            if not recompute and not upsert and cache:
                if cache_exists(backend, cache_path, verify_path,
                                hash_postgres_table_name=hash_postgres_table_name):
                    # Sync the cache from the remote to the local
                    sync_local_remote(backend, fs, read_fs, cache_path, read_cache_path, verify_path, null_path)
                    print(f"Found cache for {read_cache_path}")
                    if data_type == 'array':
                        if filepath_only:
                            return read_cache_map
                        else:
                            print(f"Opening cache {read_cache_path}")
                            # We must auto open chunks. This tries to use the underlying zarr chunking if possible.
                            # Setting chunks=True triggers what I think is an xarray/zarr engine bug where
                            # every chunk is only 4B!
                            if auto_rechunk:
                                # If rechunk is passed then check to see if the rechunk array
                                # matches chunking. If not then rechunk
                                ds_remote = xr.open_dataset(cache_map, engine='zarr', chunks={}, decode_timedelta=True)
                                if not isinstance(chunking, dict):
                                    raise ValueError(
                                        "If auto_rechunk is True, a chunking dict must be supplied.")

                                # Compare the dict to the rechunk dict
                                if not chunking_compare(ds_remote, chunking):
                                    print(
                                        "Rechunk was passed and cached chunks do not match rechunk request. "
                                        "Performing rechunking.")

                                    # write to a temp cache map
                                    # writing to temp cache is necessary because if you overwrite
                                    # the original cache map it will write it before reading the
                                    # data leading to corruption.
                                    temp_cache_path = CACHE_ROOT_DIR + 'temp/' + cache_key + '.temp'
                                    temp_verify_path = CACHE_ROOT_DIR + 'temp/' + cache_key + '.verify'
                                    chunk_to_zarr(ds_remote, temp_cache_path, temp_verify_path, chunking)

                                    # Remove the old cache and verify files
                                    if fs.exists(verify_path):
                                        fs.rm(verify_path, recursive=True)
                                    if fs.exists(cache_path):
                                        fs.rm(cache_path, recursive=True)

                                    fs.mv(temp_cache_path, cache_path, recursive=True)
                                    fs.mv(temp_verify_path, verify_path, recursive=True)

                                    # Sync the cache from the remote to the local
                                    sync_local_remote(backend, fs, read_fs, cache_path,
                                                      read_cache_path, verify_path, null_path)

                                    # Reopen the dataset - will use the appropriate global or local cache
                                    ds = xr.open_dataset(read_cache_map, engine='zarr',
                                                         chunks={}, decode_timedelta=True)
                                else:
                                    # Requested chunks already match rechunk.
                                    ds = xr.open_dataset(read_cache_map, engine='zarr',
                                                         chunks={}, decode_timedelta=True)
                            else:
                                ds = xr.open_dataset(read_cache_map, engine='zarr', chunks={}, decode_timedelta=True)

                            if validate_cache_timeseries and timeseries is not None:
                                # Check to see if the dataset extends roughly the full time series set
                                match_time = [t for t in tl if t in ds.dims]
                                if len(match_time) == 0:
                                    raise RuntimeError("Timeseries array functions must return "
                                                       "a time dimension for slicing. "
                                                       "This could be an invalid cache. "
                                                       "Try running with recompute=True to reset the cache.")
                                else:
                                    time_col = match_time[0]

                                    # Assign start and end times if None are passed
                                    st = dateparser.parse(start_time) if start_time is not None \
                                        else pd.Timestamp(ds[time_col].min().values)
                                    et = dateparser.parse(end_time) if end_time is not None \
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
                    elif data_type == 'tabular':
                        if backend == 'delta':
                            if filepath_only:
                                return read_cache_map
                            else:
                                print(f"Opening cache {read_cache_path}")
                                ds = read_from_delta(read_cache_path)

                                if validate_cache_timeseries and timeseries is not None:
                                    raise NotImplementedError("""Timeseries validation is not currently implemented
                                                              for tabular datasets.""")
                                else:
                                    compute_result = False
                        elif backend == 'parquet':
                            if filepath_only:
                                return read_cache_path
                            else:
                                print(f"Opening cache {read_cache_path}")
                                ds = read_from_parquet(read_cache_path)

                                if validate_cache_timeseries and timeseries is not None:
                                    raise NotImplementedError("""Timeseries validation is not currently implemented
                                                              for tabular datasets.""")
                                else:
                                    compute_result = False

                        elif backend == 'postgres':
                            ds = read_from_postgres(cache_key, hash_table_name=hash_postgres_table_name)
                            if validate_cache_timeseries and timeseries is not None:
                                raise NotImplementedError("""Timeseries validation is not currently implemented
                                                          for tabular datasets""")
                            else:
                                compute_result = False
                        else:
                            raise ValueError("""Only delta, parquet, and postgres backends are
                                             supported for tabular data.""")
                    elif data_type == 'basic':
                        if backend == 'pickle':
                            if filepath_only:
                                return read_cache_map
                            else:
                                print(f"Opening cache {read_cache_path}")
                                with read_fs.open(read_cache_path, 'rb') as f:
                                    ds = pickle.load(f)

                                if validate_cache_timeseries and timeseries is not None:
                                    raise NotImplementedError("""Timeseries validation is not currently implemented
                                                              for basic datasets.""")
                                else:
                                    compute_result = False
                        else:
                            raise ValueError("Only pickle backend supported for basic types.")
                    else:
                        print("Auto caching currently only supports array, tabular, and basic types.")
                elif fs.exists(null_path) and not recompute and cache and not retry_null_cache:
                    print(f"Found null cache for {null_path}. Skipping computation.")
                    return None

            # If the cache doesn't exist or we are recomputing, compute the result
            if compute_result or backend != storage_backend:
                if compute_result:
                    if recompute:
                        print(f"Recompute for {cache_key} requested. Not checking for cached result.")
                    elif upsert:
                        print(f"Computing {cache_key} to enable data upsert.")
                    elif not cache:
                        # The function isn't cacheable, recomputing
                        pass
                    else:
                        if fail_if_no_cache:
                            raise RuntimeError(f"""Computation has been disabled by
                                                `fail_if_no_cache` and cache doesn't exist for {cache_key}.""")

                        print(f"Cache doesn't exist for {cache_key}. Running function")

                    if timeseries is not None and (start_time is None or end_time is None):
                        raise ValueError('Need to pass start and end time arguments when recomputing function.')

                    ##### IF NOT EXISTS ######
                    ds = func(*args, **kwargs)
                    ##########################

                # Store the result
                if cache:
                    if ds is None:
                        if not upsert:
                            print(f"Autocaching null result for {null_path}.")
                            with fs.open(null_path, 'wb') as f:
                                f.write(b'')
                                sync_local_remote(backend, fs, read_fs, cache_path, read_cache_path,
                                                  verify_path, null_path)
                        else:
                            print(f"Null result not cached for {null_path} in upsert mode.")

                        return None

                    write = False  # boolean to determine if we should write to the cache
                    if data_type == 'array':
                        if storage_backend == 'zarr':
                            if (cache_exists(storage_backend, cache_path, verify_path)
                                and force_overwrite is None):
                                inp = input(f'A cache already exists at {
                                            cache_path}. Are you sure you want to overwrite it? (y/n)')
                                if inp == 'y' or inp == 'Y':
                                    write = True
                            elif force_overwrite is False:
                                pass
                            else:
                                write = True

                            if write:
                                print(f"Caching result for {cache_path} as zarr.")
                                if isinstance(ds, xr.Dataset):
                                    chunk_config = chunking if chunking else 'auto'
                                    chunk_to_zarr(ds, cache_path, verify_path, chunk_config)
                                    sync_local_remote(backend, fs, read_fs, cache_path,
                                                      read_cache_path, verify_path, null_path)
                                    # Reopen the dataset to truncate the computational path
                                    ds = xr.open_dataset(read_cache_map, engine='zarr',
                                                         chunks={}, decode_timedelta=True)
                                else:
                                    raise RuntimeError(
                                        f"Array datatypes must return xarray datasets or None instead of {type(ds)}")
                        # Either way if terracotta is specified as a backend try to write the result array to terracotta
                        elif storage_backend == 'terracotta':
                            print(f"Also caching {cache_key} to terracotta.")
                            write_to_terracotta(cache_key, ds)
                    elif data_type == 'tabular':
                        if not (isinstance(ds, pd.DataFrame) or isinstance(ds, dd.DataFrame)):
                            raise RuntimeError(f"""Tabular datatypes must return pandas or dask dataframe
                                               or none instead of {type(ds)}""")

                        # TODO: combine repeated code
                        if storage_backend == 'delta':
                            if fs.exists(cache_path) and force_overwrite is None:
                                inp = input(f'A cache already exists at {
                                            cache_path}. Are you sure you want to overwrite it? (y/n)')
                                if inp == 'y' or inp == 'Y':
                                    write = True
                            elif force_overwrite is False:
                                pass
                            else:
                                write = True

                            if write:
                                print(f"Caching result for {cache_path} in delta.")
                                write_to_delta(ds, cache_path, overwrite=True)
                                # Reopen dataset to truncate the computational path
                                ds = read_from_delta(read_cache_path)
                        elif storage_backend == 'parquet':
                            if (
                                cache_exists(
                                    storage_backend,
                                    cache_path,
                                    verify_path,
                                )
                                and force_overwrite is None
                                and not upsert
                            ):
                                inp = input(f'A cache already exists at {
                                            cache_path}. Are you sure you want to overwrite it? (y/n)')
                                if inp == 'y' or inp == 'Y':
                                    write = True
                            else:
                                write = True

                            if write:
                                print(f"Caching result for {cache_path} in parquet.")
                                write_to_parquet(ds, cache_path, verify_path, overwrite=True,
                                                 upsert=upsert, primary_keys=primary_keys)
                                sync_local_remote(backend, fs, read_fs, cache_path,
                                                  read_cache_path, verify_path, null_path)
                                # Reopen dataset to truncate the computational path
                                ds = read_from_parquet(read_cache_path)

                        elif storage_backend == 'postgres':
                            if (check_exists_postgres(cache_key, hash_table_name=hash_postgres_table_name)
                               and force_overwrite is None and not upsert):
                                inp = input(f'A cache already exists at {
                                            cache_path}. Are you sure you want to overwrite it? (y/n)')
                                if inp == 'y' or inp == 'Y':
                                    write = True
                            elif force_overwrite is False:
                                pass
                            else:
                                write = True

                            if write:
                                print(f"Caching result for {cache_key} in postgres.")
                                write_to_postgres(ds, cache_key, overwrite=True, upsert=upsert,
                                                  primary_keys=primary_keys, hash_table_name=hash_postgres_table_name)
                                # Don't support computation path truncation because dask read sql function
                                # requires knowledge of indexes we don't have generically
                                # ds = read_from_postgres(cache_path)
                                return ds
                        else:
                            raise ValueError("Only delta and postgres backends are implemented for tabular data")
                    elif data_type == 'basic':
                        if backend == 'pickle':
                            if fs.exists(cache_path) and force_overwrite is None:
                                inp = input(f'A cache already exists at {
                                            cache_path}. Are you sure you want to overwrite it? (y/n)')
                                if inp == 'y' or inp == 'Y':
                                    write = True
                            elif force_overwrite is False:
                                pass
                            else:
                                write = True

                            if write:
                                print(f"Caching result for {cache_path}.")
                                with fs.open(cache_path, 'wb') as f:
                                    pickle.dump(ds, f)
                                    fs.open(verify_path, 'w').write(
                                        datetime.datetime.now(datetime.timezone.utc).isoformat())
                                sync_local_remote(backend, fs, read_fs, cache_path,
                                                  read_cache_path, verify_path, null_path)
                        else:
                            raise ValueError("Only pickle backend is implemented for basic data data")

            if filepath_only:
                if backend == 'parquet':
                    return read_cache_path
                else:
                    return read_cache_map
            else:
                # Do the time series filtering
                if timeseries is not None:

                    if data_type == "array":
                        match_time = [t for t in tl if t in ds.dims]
                    elif data_type == "tabular":
                        match_time = [t for t in tl if t in ds.columns]
                    else:
                        raise ValueError(
                            f"Timeseries is only supported for array or tabular data, not {data_type}"
                        )

                    if len(match_time) == 0:
                        raise RuntimeError(
                            f"Timeseries must have a dimension or column named {tl} for slicing."
                        )

                    time_col = match_time[0]

                    # Assign start and end times if None are passed
                    if data_type == 'array' and isinstance(ds, xr.Dataset):
                        ds = ds.sel({time_col: slice(start_time, end_time)})
                    elif data_type == 'tabular' and (isinstance(ds, pd.DataFrame) or isinstance(ds, dd.DataFrame)):
                        if start_time is not None:
                            ds = ds[ds[time_col] >= start_time]
                        if end_time is not None:
                            ds = ds[ds[time_col] <= end_time]

                return ds

        return wrapper
    return create_cacheable
