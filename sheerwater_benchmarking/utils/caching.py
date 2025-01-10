"""Automated dataframe caching utilities."""
from abc import ABC

import datetime
import dateparser
from functools import wraps
from inspect import signature, Parameter
import logging
import hashlib
import inspect

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
import xarray as xr

from sheerwater_benchmarking.utils.data_utils import lon_base_change
from sheerwater_benchmarking.utils.secrets import postgres_write_password, postgres_read_password

logger = logging.getLogger(__name__)

# Chunk size limits
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
    return np.product(chunk_sizes) * 4 / div, chunk_groups


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


def postgres_table_name(table_name):
    """Return a qualified postgres table name."""
    return hashlib.md5(table_name.encode()).hexdigest()


class cacheable(ABC):
    """Decorator for handling typed data loading, storing, and composable computation.

    Args:
        data_type(str): The type of data being cached. Currently only 'array' is supported.
        cache_args(list): The arguments to use as the cache key.
        timeseries(str, list): The name of the time series dimension in the cached array. If not a
            time series, set to None (default). If a list, will use the first matching coordinate in the list.
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
    #  Default parameters specifying cache behavior at runtime
    default_cache_params = {
        "filepath_only": False,
        "recompute": False,
        "dont_recompute": None,
        "cache": None,
        "validate_cache_timeseries": True,
        "force_overwrite": False,
        "retry_null_cache": False,
        "backend": None,
        "storage_backend": None,
        "auto_rechunk":  None,
    }
    data_backends = {
        'array': 'zarr',
        'tabular': 'delta',
        'basic': 'pickle'
    }
    backend_defaults = {
        'zarr': {
            'supports_filepath': True,
        },
        'delta': {
            'supports_filepath': True,
        },
        'pickle': {
            'supports_filepath': True,
        },
        'postgres': {
            'supports_filepath': False
        },
        'terracotta': {
            'supports_filepath': False
        }
    }

    def __init__(self, data_type, cache_args, timeseries=None, chunking=None, chunk_by_arg=None,
                 auto_rechunk=False, cache=True, cache_disable_if=None,
                 backend=None, storage_backend=None):
        # Initialize the google filesystem
        self.fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')

        # Set the config from the decorator initialization scope
        self.config = {
            'data_type': data_type,
            'cache_args': cache_args,
            'timeseries': timeseries,
            'chunking': chunking,
            'chunk_by_arg': chunk_by_arg,
            'auto_rechunk': auto_rechunk,
            'cache': cache,
            'cache_disable_if': cache_disable_if,
            'backend': backend,
            'storage_backend': storage_backend
        }

    def merge_cache_args_from_scope(self, kwargs, scope, cache_kwargs):
        """Extract the cache arguments from the scope and return them."""
        for k in cache_kwargs:
            if k in scope:
                # Get the cache arg from the scope
                kwargs[k] = scope[k]
        return kwargs

    def read_from_delta(self, cache_path):
        """Read from a deltatable into a pandas dataframe."""
        return DeltaTable(cache_path).to_pandas()

    def write_to_delta(self, cache_path, df, overwrite=False):
        """Write a pandas dataframe to a delta table."""
        if overwrite:
            write_deltalake(cache_path, df, mode='overwrite', schema_mode='overwrite')
        else:
            write_deltalake(cache_path, df)

    def write_to_zarr(self, ds, cache_path, verify_path):
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
        if self.fs.exists(verify_path):
            self.fs.rm(verify_path, recursive=True)

        if self.fs.exists(cache_path):
            self.fs.rm(cache_path, recursive=True)

        cache_map = self.fs.get_mapper(cache_path)
        ds.to_zarr(store=cache_map, mode='w')

        self.fs.touch(verify_path)

    def chunk_to_zarr(self, ds, cache_path, verify_path, chunking):
        """Write a dataset to a zarr cache map and check the chunking."""
        ds = drop_encoded_chunks(ds)

        if isinstance(chunking, dict):
            # No need to prune if chunking is None or 'auto'
            chunking = prune_chunking_dimensions(ds, chunking)

        ds = ds.chunk(chunks=chunking)
        chunk_size, chunk_with_labels = get_chunk_size(ds)

        if chunk_size > CHUNK_SIZE_UPPER_LIMIT_MB or chunk_size < CHUNK_SIZE_LOWER_LIMIT_MB:
            print(f"WARNING: Chunk size is {chunk_size}MB. Target approx 100MB.")
            print(chunk_with_labels)

        self.write_to_zarr(ds, cache_path, verify_path)

    def read_from_postgres(self, table_name, hash_table_name=True):
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

    def check_exists_postgres(self, table_name):
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

    def write_to_postgres(self, pandas_df, table_name, overwrite=False):
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

    def write_to_terracotta(self, cache_key, ds):
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
        if not hasattr(self.write_to_terracotta, 'driver'):
            driver = tc.get_driver("postgresql://sheerwater-benchmarking-postgres:5432/terracotta")
            self.write_to_terracotta.driver = driver
        else:
            driver = self.write_to_terracotta.driver

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

    def get_cache_config(self, kwargs, func):
        """Get the final cache configuration from the passed kwargs."""
        # Merge the passed cache configuration with the internal cache configuration
        cache_config = self.config.copy()
        cache_config = self.get_cache_args(cache_config, kwargs, self.default_cache_params)

        # Extract the cache configuration from the passed kwargs
        if cache_config['dont_recompute'] is not None and func.__name__ in cache_config['dont_recompute']:
            # If this function is in the dont_recompute list then override recompute
            cache_config['recompute'] = False

        return cache_config

    def get_cache_args(self, cache_config, passed_kwargs, cache_config_kwargs):
        """Extract the cache arguments from the kwargs and return them."""
        for k in cache_config_kwargs:
            if k in passed_kwargs and passed_kwargs[k] is not None:
                # Overwrite the cache config parameter with the passed kwargs
                # if provided and is not None
                cache_config[k] = passed_kwargs[k]
                # Remove cache config parameter from the passed kwargs
                del passed_kwargs[k]
            else:
                # If the cache config parameter is not in the passed kwargs
                # then set it to the default value
                cache_config[k] = cache_config_kwargs[k]
        return cache_config

    def validate_config(self, cache_config, params, args, kwargs):
        """Validate the passed configuration.

        TODO: this should be implemented in type-specific subclasses. We can also
        add kwarg validation here.
        """
        if cache_config['timeseries'] is not None:
            # Validate time series params
            start_time = None
            end_time = None

            # Convert to a list if not
            timeseries = cache_config['timeseries']
            timeseries = timeseries if isinstance(timeseries, list) else [timeseries]

            if 'start_time' in cache_config['cache_args'] or 'end_time' in cache_config['cache_args']:
                raise ValueError(
                    "Time series functions must not place their time arguments in cacheable_args!")

            if 'start_time' not in params or 'end_time' not in params:
                raise ValueError(
                    "Time series functions must have the parameters 'start_time' and 'end_time'")
            else:
                keys = [item for item in params]
                start_time = args[keys.index('start_time')]
                end_time = args[keys.index('end_time')]

            # Add the start and end time to the cache config
            cache_config['start_time'] = start_time
            cache_config['end_time'] = end_time
            cache_config['timeseries'] = timeseries

        if cache_config['data_type'] not in ['array']:
            # Timeseries validation supported
            if cache_config['validate_cache_timeseries'] and cache_config['timeseries'] is not None:
                raise NotImplementedError("""Timeseries validation is only currently implemented
                                            for array datasets.""")

        if cache_config['validate_cache_timeseries'] and cache_config['timeseries'] is None:
            raise ValueError("Must specify a timeseries dimension to validate cache timeseries.")

        return cache_config

    def get_passed_cache_args(self, cache_config, params, args, kwargs):
        """Get the cacheable arguments from the passed arguments, handling defaults."""
        # Handle keying based on cache arguments
        cache_arg_values = {}
        for a in cache_config['cache_args']:
            # If it was passed in as a kwarg, then use that directly
            if a in kwargs:
                cache_arg_values[a] = kwargs[a]
                continue

            # If it's not in kwargs it must either be (1) in args or (2) passed as default
            found = False
            for i, p in enumerate(params):
                if (a == p and len(args) > i and  # if found in arg list and we've passed enough args
                        (params[p].kind == Parameter.VAR_POSITIONAL or
                            params[p].kind == Parameter.POSITIONAL_OR_KEYWORD)):
                    # Found as (1) in the argument list
                    cache_arg_values[a] = args[i]
                    found = True
                    break
                elif a == p and params[p].default != Parameter.empty:  # if found in default args
                    # Found as (2) in the default value
                    cache_arg_values[a] = params[p].default
                    found = True
                    break

            if not found:
                raise RuntimeError(f"Specified cacheable argument {a} "
                                   "not discovered as passed argument or default argument.")
        return cache_arg_values

    def update_cache_config(self, cache_config, cache_arg_values):
        """Update the caching config based on passed values

        Args:
            cache_config (dict): The cache configuration.
            cache_arg_values (dict): The cache argument values.
        """
        # Merge the chunking with the passed argument chunking modifiers
        if cache_config['chunk_by_arg'] is not None:
            for k in cache_config['chunk_by_arg']:
                if k not in cache_arg_values:
                    raise ValueError(f"Chunking modifier {k} not found in cache arg values.")

                if cache_arg_values[k] in cache_config['chunk_by_arg'][k]:
                    # If argument value in chunk_by_arg then merge the chunking
                    chunk_dict = cache_config['chunk_by_arg'][k][cache_arg_values[k]]
                    cache_config['chunking'].update(chunk_dict)

        # Now that we have all the cacheable args values we can calculate whether
        # the cache should be disabled from them
        cache_disable_if = cache_config['cache_disable_if']
        if isinstance(cache_disable_if, dict) or isinstance(cache_disable_if, list):
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

                # If they match then disable caching
                if comp_arg_values == d:
                    print(f"Caching disabled for arg values {d}")
                    cache_config['cache'] = False
                    break
        elif cache_disable_if is not None:
            raise ValueError("cache_disable_if only accepts a dict or list of dicts.")

        return cache_config

    def cache_key(self, func, cache_arg_values):
        """Generate a cache key from the cache argument values."""
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
        return cache_key

    def get_backend_config(self, cache_config):
        """Get the relevant backend storage info and modify the cache config in place."""
        # Unpack the cache configuration
        cache_key = cache_config['cache_key']

        # Get the data type
        data_type = cache_config['data_type']
        if data_type not in self.data_backends:
            raise ValueError(f"Data type {data_type} not supported for caching.")

        # Get the backend (with defaults) for the data type
        backend = cache_config['backend']
        if backend is None:
            # Get the default backend for the data type
            backend = self.data_backends[data_type]
        storage_backend = cache_config['storage_backend']
        if storage_backend is None:
            storage_backend = backend

        # Get the backend storage locations
        supports_filepath = self.backend_defaults[backend]['supports_filepath']
        cache_prefix = 'gs://sheerwater-datalake/caches/' + cache_key
        null_path = cache_prefix + '.null'
        if backend == 'zarr':
            cache_path = cache_prefix + '.zarr'
            verify_path = cache_prefix + '.verify'
        elif backend == 'delta':
            cache_path = cache_prefix + '.delta'
            verify_path = None
        elif backend == 'postgres':
            cache_path = cache_key
            verify_path = None
        elif backend == 'pickle':
            cache_path = cache_prefix + '.pkl'
            verify_path = None
        else:
            raise ValueError("Backend {backend} not supported for caching.")

        if cache_config['filepath_only'] and not supports_filepath:
            raise ValueError(f"{backend} backend does not support filepath_only flag")

        # Update cache config with the backend storage info
        cache_config['cache_path'] = cache_path
        cache_config['verify_path'] = verify_path
        cache_config['null_path'] = null_path
        cache_config['backend'] = backend
        cache_config['storage_backend'] = storage_backend
        return cache_config

    def cache_exists(self, backend, cache_path, verify_path=None):
        """Check if a cache exists generically."""
        if backend in ['zarr', 'delta', 'pickle', 'terracotta']:
            if verify_path is not None:
                if not self.fs.exists(verify_path) and self.fs.exists(cache_path):
                    print("Found cache, but it appears to be corrupted. Recomputing.")
                    return False
                elif self.fs.exists(verify_path) and self.fs.exists(cache_path):
                    return True
                else:
                    return False
            else:
                return self.fs.exists(cache_path)
        elif backend == 'postgres':
            return self.check_exists_postgres(cache_path)

    def load_cache(self, config):
        """Load the cache generically.

        TODO: should be split into subclass specific implementations for each type. 

        Returns:
            ds (xr.Dataset): The loaded dataset. None if the loaded cache is invalid and should be recomputed.
        """
        data_type = config['data_type']
        cache_path = config['cache_path']
        verify_path = config['verify_path']
        filepath_only = config['filepath_only']
        backend = config['backend']

        if data_type == 'array':
            cache_map = self.fs.get_mapper(cache_path)
            if filepath_only:
                return cache_map

            print(f"Opening cache {cache_path}")
            # We must auto open chunks. This tries to use the underlying zarr chunking if possible.
            # Setting chunks=True triggers what I think is an xarray/zarr engine bug where
            # every chunk is only 4B!
            ds = xr.open_dataset(cache_map, engine='zarr', chunks={})

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
                    temp_cache_path = 'gs://sheerwater-datalake/caches/temp/' + config['cache_key'] + '.temp'
                    temp_verify_path = 'gs://sheerwater-datalake/caches/temp/' + config['cache_key'] + '.verify'
                    self.chunk_to_zarr(ds, temp_cache_path, temp_verify_path, config['chunking'])

                    if self.fs.exists(verify_path):
                        self.fs.rm(verify_path, recursive=True)

                    if self.fs.exists(cache_path):
                        self.fs.rm(cache_path, recursive=True)

                    self.fs.mv(temp_cache_path, cache_path, recursive=True)
                    self.fs.mv(temp_verify_path, verify_path, recursive=True)

                    # Reopen the dataset
                    ds = xr.open_dataset(cache_map, engine='zarr', chunks={})
                else:
                    # Requested chunks already match rechunk.
                    pass

            if not config['validate_cache_timeseries']:
                return ds
            else:
                # Check to see if the dataset extents roughly the full time series
                match_time = [time for time in config['timeseries'] if time in ds.dims]
                if len(match_time) == 0:
                    raise RuntimeError("Timeseries array functions must return "
                                       "a time dimension for slicing. "
                                       "This could be an invalid cache. "
                                       "Try running with recompute=True to reset the cache.")

                # Take the first matching time dimension
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
                    print("WARNING: The cached array does not have data within "
                          "1 year of your start or end time. Automatically recomputing. "
                          "If you do not want to recompute the result set "
                          "`validate_cache_timeseries=False`")
                    return None
                else:
                    # We have a valid time series, chunked cache. Return.
                    return ds
        elif data_type == 'tabular':
            if backend == 'delta':
                if filepath_only:
                    cache_map = self.fs.get_mapper(cache_path)
                    return cache_map
                else:
                    print(f"Opening cache {cache_path}")
                    ds = self.read_from_delta(cache_path)
                    return ds
            elif backend == 'postgres':
                ds = self.read_from_postgres(config['cache_key'])
                return ds
            else:
                raise ValueError("Tabular data type only supports delta and postgres backends.")
        elif data_type == 'basic':
            if backend == 'pickle':
                if filepath_only:
                    cache_map = self.fs.get_mapper(cache_path)
                    return cache_map
                else:
                    print(f"Opening cache {cache_path}")
                    with self.fs.open(cache_path) as f:
                        ds = pickle.load(f)
                    return ds
            else:
                raise ValueError("Basic data type only supports pickle backend.")
        else:
            print("Auto caching currently only supports array, tabular, and basic types.")

    def store_cache(self, config, ds):
        """Store the cache generically.

        TODO: should be split into subclass specific implementations for each type. 

        Returns: None
        """
        data_type = config['data_type']
        cache_path = config['cache_path']
        verify_path = config['verify_path']
        backend = config['backend']
        storage_backend = config['storage_backend']

        if data_type == 'array':
            if storage_backend == 'zarr':
                write = False
                if self.cache_exists(storage_backend, cache_path, verify_path) and not config['force_overwrite']:
                    inp = input(f'A cache already exists at {
                                cache_path}. Are you sure you want to overwrite it? (y/n)')
                    if inp == 'y' or inp == 'Y':
                        write = True
                else:
                    write = True

                if write:
                    # Write the cache result
                    print(f"Caching result for {cache_path} as zarr.")
                    if isinstance(ds, xr.Dataset):
                        cache_map = self.fs.get_mapper(cache_path)

                        if config['chunking']:
                            # If we aren't doing auto chunking delete the encoding chunks
                            self.chunk_to_zarr(ds, cache_path, verify_path, config['chunking'])

                            # Reopen the dataset to truncate the computational path
                            ds = xr.open_dataset(cache_map, engine='zarr', chunks={})
                        else:
                            self.chunk_to_zarr(ds, cache_path, verify_path, 'auto')

                            # Reopen the dataset to truncate the computational path
                            ds = xr.open_dataset(cache_map, engine='zarr', chunks={})
                    else:
                        raise RuntimeError(
                            f"Array datatypes must return xarray datasets or None instead of {type(ds)}")

            # If terracotta is specified as a backend try to write the result array to terracotta
            elif storage_backend == 'terracotta':
                print(f"Also caching {config['cache_key']} to terracotta.")
                self.write_to_terracotta(config['cache_key'], ds)

        elif data_type == 'tabular':
            if not isinstance(ds, pd.DataFrame):
                raise RuntimeError(f"""Tabular datatypes must return pandas dataframe
                                    or none instead of {type(ds)}""")

            if storage_backend == 'delta':
                write = False
                if self.fs.exists(cache_path) and not config['force_overwrite']:
                    inp = input(f'A cache already exists at {
                                cache_path}. Are you sure you want to overwrite it? (y/n)')
                    if inp == 'y' or inp == 'Y':
                        write = True
                else:
                    write = True

                if write:
                    print(f"Caching result for {cache_path} in delta.")
                    self.write_to_delta(cache_path, ds, overwrite=True)

            elif storage_backend == 'postgres':
                write = False
                if self.check_exists_postgres(config['cache_key']) and not config['force_overwrite']:
                    inp = input(f'A cache already exists at {
                                cache_path}. Are you sure you want to overwrite it? (y/n)')
                    if inp == 'y' or inp == 'Y':
                        write = True
                else:
                    write = True

                if write:
                    print(f"Caching result for {config['cache_key']} in postgres.")
                    self.write_to_postgres(ds, config['cache_key'], overwrite=True)
            else:
                raise ValueError("Only delta and postgres backends are implemented for tabular data")
        elif data_type == 'basic':
            if backend == 'pickle':
                write = False
                if self.fs.exists(cache_path) and not config['force_overwrite']:
                    inp = input(f'A cache already exists at {
                                cache_path}. Are you sure you want to overwrite it? (y/n)')
                    if inp == 'y' or inp == 'Y':
                        write = True
                else:
                    write = True

                if write:
                    print(f"Caching result for {cache_path}.")
                    with self.fs.open(cache_path, 'wb') as f:
                        pickle.dump(ds, f)
            else:
                raise ValueError("Only pickle backend is implemented for basic data data")

    def process_ret(self, ret, config):
        """Process the return value from the function."""
        if config['data_type'] == 'array':
            # Perform any necessary time series slicing
            if config['timeseries'] is not None:
                match_time = [time for time in config['timeseries'] if time in ret.dims]
                if len(match_time) == 0:
                    raise RuntimeError(
                        "Timeseries array must return a 'time' dimension for slicing.")

                # Take the first matching time dimension
                time_col = match_time[0]

                # Assign start and end times if None are passed
                if config['start_time'] is None:
                    start_time = ret[time_col].min().values
                if config['end_time'] is None:
                    end_time = ret[time_col].max().values

                ret = ret.sel({time_col: slice(start_time, end_time)})
        return ret

    def __call__(self, func):
        @wraps(func)
        def cacheable_wrapper(*args, **kwargs):
            # Get the function parameters
            params = signature(func).parameters

            # Extract the cache configuration from the passed kwargs
            # Modifies kwargs in place to remove cache configuration kwargs
            config = self.get_cache_config(kwargs, func)

            # Validate the cache configuration
            config = self.validate_config(config, params, args, kwargs)

            # Get the cache arguments from the passed arguments
            cache_arg_values = self.get_passed_cache_args(config, params, args, kwargs)

            # Now that we have cache arguments, update cache configuration (merge chunk by arg, cache disable)
            config = self.update_cache_config(config, cache_arg_values)

            # Get the cache key from the cache argument values
            config['cache_key'] = self.cache_key(func, cache_arg_values)

            # Get the relevant backend storage info
            config = self.get_backend_config(config)

            ##########################
            # BEGIN CACHING LOGIC ####
            ##########################
            ret = None  # Placeholder for the return value

            # Delete the null cache if retry null cache is passed
            if self.fs.exists(config['null_path']) and config['retry_null_cache']:
                print(f"Removing and retrying null cache {config['null_path']}.")
                self.fs.rm(config['null_path'], recursive=True)

            # If we have a cacheable function and we're not recomputing, try to load the cache
            if config['cache'] and not config['recompute']:
                if self.fs.exists(config['null_path']) and not config['retry_null_cache']:
                    print(f"Found null cache for {config['null_path']}. Skipping computation.")
                    return None
                elif self.cache_exists(config['backend'], config['cache_path'], config['verify_path']):
                    # Read the cache
                    print(f"Found cache for {config['cache_path']}. Loading.")
                    ret = self.load_cache(config)

            # We need to write and update the cache
            if config['cache'] and (ret is None or config['storage_backend'] != config['backend']):
                # If we didn't get a valid read from the cache
                if ret is None:
                    if config['recompute']:
                        print(f"Recompute for {config['cache_key']} requested. Not checking for cached result.")
                    elif not config['cache']:
                        pass  # The function isn't cacheable, recomputing
                    else:
                        print(f"Cache doesn't exist for {config['cache_key']}. Running function")

                    # Check arguments for timeseries are valid when recomputing
                    if config['timeseries'] is not None and (config['start_time'] is None or config['end_time'] is None):
                        raise ValueError('Need to pass start and end time arguments when recomputing function.')

                    ##### (RE)RUN THE CACHED FUNCTION ######
                    ret = func(*args, **kwargs)
                    ########################################

                # Write the result to the cache
                if ret is None:
                    print(f"Autocaching null result for {config['null_path']}.")
                    with self.fs.open(config['null_path'], 'wb') as f:
                        f.write(b'')
                        return None
                # Write results to cache
                self.store_cache(config, ret)

            if config['filepath_only']:
                # We only want the filepath, so we can return early
                cache_map = self.fs.get_mapper(config['cache_path'])
                return cache_map

            # Process the return value
            if ret is not None:
                ret = self.process_ret(ret, config)

            # Return the final result
            return ret
        return cacheable_wrapper


def cacheable_hold(data_type, cache_args, timeseries=None, chunking=None, chunk_by_arg=None,
                   auto_rechunk=False, cache=True, cache_disable_if=None,
                   backend=None, storage_backend=None):
    """Decorator for caching function results."""
    # Valid configuration kwargs for the cacheable decorator

    def create_cacheable(func):
        @wraps(func)
        def cacheable_wrapper(*args, **kwargs):
            # Proper variable scope for the decorator args
            # nonlocal data_type, cache_args, timeseries, chunking, chunk_by_arg, \
            #     auto_rechunk, cache, cache_disable_if, backend, storage_backend

            # Calculate the appropriate cache key
            # filepath_only, recompute, dont_recompute, passed_cache, validate_cache_timeseries, \
            #     force_overwrite, retry_null_cache, backend, \
            #     storage_backend, passed_auto_rechunk =

            # Extract the cache configuration from the passed kwargs
            # Modifies kwargs in place to remove cache configuration kwargs
            # cache_config = get_cache_args(kwargs, cache_config_kwargs)

            # if passed_cache is not None:
            #     cache = passed_cache
            # if passed_auto_rechunk is not None:
            #     auto_rechunk = passed_auto_rechunk
            # if dont_recompute and func.__name__ in dont_recompute:
            #     # If this function is in the dont_recompute list then override recompute
            #     passed_recompute = False
            # else:
            #     passed_recompute = recompute

            # Get the arguments passed into the first called function
            stack = inspect.stack()[::-1]  # Search from first called in reverse order
            print("Function: ", func.__name__)
            print("Stack size: ", len(stack))
            for i, s in enumerate(stack):
                first_caller = True
                print(f"Frame {i} Details:")
                print(f"  Filename: {s.filename}")
                print(f"  Line Number: {s.lineno}")
                print(f"  Function: {s.function}")
                try:
                    print(f"  Arguments: {s.frame.f_locals['args']}")
                    print(f"  Kwargs: {s.frame.f_locals['kwargs']}")
                    print(f"  Recompute: {s.frame.f_locals['recompute']}")
                except KeyError:
                    pass

                if first_caller and s.function == 'cacheable_wrapper':  # This is the first wrapper function call
                    print("  First Wrapper Function Call")
                    first_caller = False
                    import pdb
                    pdb.set_trace()
                    kwargs = merge_cache_args_from_scope(kwargs, s.frame.f_locals, cache_kwargs)
                    import pdb
                    pdb.set_trace()
                # print(f"  Code Context: {s.code_context}")
                # print(f"  Context Index: {s.index}")

        return cacheable_wrapper
    return create_cacheable
