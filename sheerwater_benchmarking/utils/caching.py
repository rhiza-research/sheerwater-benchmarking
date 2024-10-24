"""Automated dataframe caching utilities."""
import datetime
import dateparser
from functools import wraps
from inspect import signature, Parameter
import logging

import gcsfs
from google.cloud import storage
from deltalake import DeltaTable, write_deltalake
from rasterio.io import MemoryFile
from rasterio.enums import Resampling
import terracotta as tc
import sqlalchemy

import numpy as np
import pandas as pd
import xarray as xr

from sheerwater_benchmarking.utils.data_utils import lon_base_change
from sheerwater_benchmarking.utils.secrets import postgres_write_password, postgres_read_password

logger = logging.getLogger(__name__)


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
            for dim in chunk_dict:
                chunking[dim] = chunk_dict[dim]

    import pdb; pdb.set_trace()
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

    return ds


def read_from_delta(cache_path):
    """Read from a deltatable into a pandas dataframe."""
    return DeltaTable(cache_path).to_pandas()


def write_to_delta(cache_path, df):
    """Write a pandas dataframe to a delta table."""
    write_deltalake(cache_path, df)


def read_from_postgres(table_name):
    """Read a pandas df from a table in the sheerwater postgres.

    Backends should eventually be flexibly specified, but for now
    we'll just hard code a connection to the running sheerwater database.

    Args:
        table_name: The table name to read from
    """
    # Get the postgres write secret
    pgread_pass = postgres_read_password()

    try:
        engine = sqlalchemy.create_engine(
            f'postgresql://read:{pgread_pass}@sheerwater-benchmarking-postgres:5432/postgres')
        df = pd.read_sql_query(f'select * from {table_name}', con=engine)
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

    try:
        engine = sqlalchemy.create_engine(
            f'postgresql://write:{pgwrite_pass}@sheerwater-benchmarking-postgres:5432/postgres')
        if overwrite:
            pandas_df.to_sql(table_name, engine, if_exists='replace')
        else:
            pandas_df.to_sql(table_name, engine)
    except sqlalchemy.exc.InterfaceError:
        raise RuntimeError("""Error connecting to database. Make sure you are on the tailnet
                           and can see sheerwater-benchmarking-postgres.""")


def write_to_terracotta(cache_key, ds):
    """Write geospatial array to terracotta.

    Args:
        cache_key(str): The unique key to store the data in terracotta
        ds (xr.Dataset): Dataset which holds raster
    """
    # Check to make sure this is geospatial data
    lats = ['lat', 'y', 'latitude']
    lons = ['lon', 'x', 'longitude']
    if len(ds.dims) != 2:
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
        raise RuntimeError("Can only store two dimensional geospatial data to terracotta")

    # Adjust coordinates
    if (ds['x'] > 180.0).any():
        lon_base_change(ds, lon_dim='x')
        ds = ds.sortby(['x'])

    # Adapt the CRS
    ds.rio.write_crs("epsg:4326", inplace=True)
    ds = ds.rio.reproject('EPSG:3857', resampling=Resampling.nearest, nodata=np.nan)
    ds.rio.write_crs("epsg:3857", inplace=True)

    # Write the raster
    with MemoryFile() as mem_dst:
        ds.rio.to_raster(mem_dst.name, driver="COG")

        storage_client = storage.Client()
        bucket = storage_client.bucket("sheerwater-datalake")
        blob = bucket.blob(f'rasters/{cache_key}.tif')
        blob.upload_from_file(mem_dst)

        # Register with terracotta
        tc.update_settings(SQL_USER="write", SQL_PASSWORD=postgres_write_password())
        driver = tc.get_driver("postgresql://sheerwater-benchmarking-postgres:5432/terracotta")

        try:
            driver.get_keys()
        except sqlalchemy.exc.DatabaseError:
            # Create a metastore
            print("Creating new terracotta metastore")
            driver.create(['key'])

        # Insert the parameters.
        with driver.connect():
            driver.insert({'key': cache_key.replace('/', '_')}, mem_dst,
                          override_path=f'/mnt/sheerwater-datalake/{cache_key}.tif')

        print(f"Inserted {cache_key.replace('/', '_')} into the terracotta database.")


def cache_exists(backend, cache_path):
    """Check if a cache exists generically."""
    if backend == 'zarr' or backend == 'delta':
        # Check to see if the cache exists for this key
        fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
        return fs.exists(cache_path)
    elif backend == 'postgres':
        return check_exists_postgres(cache_path)


def cacheable(data_type, cache_args, timeseries=None, chunking=None, chunk_by_arg=None,
              auto_rechunk=False, cache=True, cache_disable_if=None, backend=None):
    """Decorator for caching function results.

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
        backend(str): The name of the backend to use for cache storage. None for
            default, zarr, delta, postgres, terracotta.
    """
    # Valid configuration kwargs for the cacheable decorator
    cache_kwargs = {
        "filepath_only": False,
        "recompute": False,
        "cache": None,
        "validate_cache_timeseries": True,
        "force_overwrite": False,
        "retry_null_cache": False,
        "backend": None,
    }

    def create_cacheable(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Proper variable scope for the decorator args
            nonlocal data_type, cache_args, timeseries, chunking, chunk_by_arg, \
                auto_rechunk, cache, cache_disable_if, backend

            # Calculate the appropriate cache key
            filepath_only, recompute, passed_cache, validate_cache_timeseries, \
                force_overwrite, retry_null_cache, backend = get_cache_args(
                    kwargs, cache_kwargs)

            if passed_cache is not None:
                cache = passed_cache

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
                    start_time = args[keys.index('start_time')]
                    end_time = args[keys.index('end_time')]

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

            # Now that we have all the cacheable args values we can calculate whether
            # the cache should be disable from them
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
                        cache = False
                        break
            elif cache_disable_if is not None:
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
            if data_type == 'array':
                backend = "zarr" if backend is None else backend
                cache_path = "gs://sheerwater-datalake/caches/" + cache_key + '.zarr'
                null_path = "gs://sheerwater-datalake/caches/" + cache_key + '.null'
                supports_filepath = True
            elif data_type == 'tabular':
                # Set the default
                backend = "delta" if backend is None else backend

                if backend == 'delta':
                    cache_path = "gs://sheerwater-datalake/caches/" + cache_key + '.delta'
                    null_path = "gs://sheerwater-datalake/caches/" + cache_key + '.null'
                    supports_filepath = True
                elif backend == 'postgres':
                    cache_key = cache_key.replace('/', '_')
                    cache_path = cache_key
                    null_path = "gs://sheerwater-datalake/caches/" + cache_key + '.null'
                    supports_filepath = False
            else:
                raise ValueError("Caching currently only supports the 'array' and 'tabular' datatypes")

            if filepath_only and not supports_filepath:
                raise ValueError(f"{backend} backend does not support filepath_only flag")

            ds = None
            compute_result = True
            fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')

            # Delete the null cache if retry null cache is passed
            if fs.exists(null_path) and retry_null_cache:
                print(f"Removing and retrying null cache {null_path}.")
                fs.rm(null_path, recursive=True)

            if not recompute and cache:
                if cache_exists(backend, cache_path):
                    # Read the cache
                    print(f"Found cache for {cache_path}")
                    if data_type == 'array':
                        cache_map = fs.get_mapper(cache_path)

                        if filepath_only:
                            return cache_map
                        else:
                            print(f"Opening cache {cache_path}")
                            # We must auto open chunks. This tries to use the underlying zarr chunking if possible.
                            # Setting chunks=True triggers what I think is an xarray/zarr engine bug where
                            # every chunk is only 4B!
                            ds = xr.open_dataset(cache_map, engine='zarr', chunks={})

                            # If rechunk is passed then check to see if the rechunk array
                            # matches chunking. If not then rechunk
                            if auto_rechunk:
                                if not isinstance(chunking, dict):
                                    raise ValueError(
                                        "If auto_rechunk is True, a chunking dict must be supplied.")

                                # Compare the dict to the rechunk dict
                                if not chunking_compare(ds, chunking):
                                    print(
                                        "Rechunk was passed and cached chunks do not match rechunk request. "
                                        "Performing rechunking.")

                                    # write to a temp cache map
                                    # writing to temp cache is necessary because if you overwrite
                                    # the original cache map it will write it before reading the
                                    # data leading to corruption.
                                    temp_cache_path = 'gs://sheerwater-datalake/caches/temp/' + cache_key + '.temp'
                                    temp_cache_map = fs.get_mapper(temp_cache_path)

                                    ds = drop_encoded_chunks(ds)

                                    ds.chunk(chunks=chunking).to_zarr(store=temp_cache_map, mode='w')

                                    # move to a permanent cache map
                                    if fs.exists(cache_path):
                                        print(f"Deleting {cache_path} to replace.")
                                        fs.rm(cache_path, recursive=True)
                                        print(f"Confirm deleted {cache_path} to replace.")

                                    fs.mv(temp_cache_path, cache_path, recursive=True)

                                    # Reopen the dataset
                                    ds = xr.open_dataset(cache_map, engine='zarr', chunks={})
                                else:
                                    # Requested chunks already match rechunk.
                                    pass

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
                                cache_map = fs.get_mapper(cache_path)

                                return cache_map
                            else:
                                print(f"Opening cache {cache_path}")
                                ds = read_from_delta(cache_path)

                                if validate_cache_timeseries and timeseries is not None:
                                    raise NotImplementedError("""Timeseries validation is not currently implemented
                                                              for tabular datasets.""")
                                else:
                                    compute_result = False
                        elif backend == 'postgres':
                            ds = read_from_postgres(cache_key)
                            if validate_cache_timeseries and timeseries is not None:
                                raise NotImplementedError("""Timeseries validation is not currently implemented
                                                          for tabular datasets""")
                            else:
                                compute_result = False
                        else:
                            raise ValueError("""Only delta, and postgres backends are
                                             supported for tabular data.""")
                    else:
                        print("Auto caching currently only supports array and tabular types.")
                elif fs.exists(null_path) and not recompute and cache and not retry_null_cache:
                    print(f"Found null cache for {null_path}. Skipping computation.")
                    return None

            if compute_result:
                if recompute:
                    print(f"Recompute for {cache_key} requested. Not checking for cached result.")
                elif not cache:
                    # The function isn't cacheable, recomputing
                    pass
                else:
                    print(f"Cache doesn't exist for {cache_key}. Running function")

                if timeseries is not None and (start_time is None or end_time is None):
                    raise ValueError('Need to pass start and end time arguments when recomputing function.')

                ##### IF NOT EXISTS ######
                ds = func(*args, **kwargs)
                ##########################

                # Store the result
                if cache:
                    if ds is None:
                        print(f"Autocaching null result for {null_path}.")
                        with fs.open(null_path, 'wb') as f:
                            f.write(b'')
                            return None

                    if data_type == 'array':
                        write = False
                        if fs.exists(cache_path) and not force_overwrite:
                            inp = input(f'A cache already exists at {
                                        cache_path}. Are you sure you want to overwrite it? (y/n)')
                            if inp == 'y' or inp == 'Y':
                                write = True
                        else:
                            write = True

                        if write:
                            print(f"Caching result for {cache_path}.")
                            if isinstance(ds, xr.Dataset):
                                cache_map = fs.get_mapper(cache_path)

                                if chunking:
                                    # If we aren't doing auto chunking delete the encoding chunks
                                    ds = drop_encoded_chunks(ds)

                                    chunking = merge_chunk_by_arg(chunking, chunk_by_arg, cache_arg_values)
                                    chunking = prune_chunking_dimensions(ds, chunking)

                                    ds.chunk(chunks=chunking).to_zarr(store=cache_map, mode='w')

                                    # Reopen the dataset to truncate the computational path
                                    ds = xr.open_dataset(cache_map, engine='zarr', chunks={})
                                else:
                                    chunks = 'auto'
                                    ds.chunk(chunks=chunks).to_zarr(store=cache_map, mode='w')

                                    # Reopen the dataset to truncate the computational path
                                    ds = xr.open_dataset(cache_map, engine='zarr', chunks={})
                            else:
                                raise RuntimeError(
                                    f"Array datatypes must return xarray datasets or None instead of {type(ds)}")

                        # Either way if terracotta is specified as a backend try to write the result array to terracotta
                        if backend == 'terracotta':
                            print(f"Also caching {cache_key} to terracotta.")
                            write_to_terracotta(cache_key, ds)

                    elif data_type == 'tabular':
                        if not isinstance(ds, pd.DataFrame):
                            raise RuntimeError(f"""Tabular datatypes must return pandas dataframe
                                               or none instead of {type(ds)}""")

                        if backend == 'delta':
                            write = False
                            if fs.exists(cache_path) and not force_overwrite:
                                inp = input(f'A cache already exists at {
                                            cache_path}. Are you sure you want to overwrite it? (y/n)')
                                if inp == 'y' or inp == 'Y':
                                    write = True
                            else:
                                write = True

                            if write:
                                print(f"Caching result for {cache_path}.")
                                write_to_delta(cache_path, ds)

                        elif backend == 'postgres':
                            print(f"Caching result for {cache_key} in postgres.")

                            write = False
                            if check_exists_postgres(cache_key) and not force_overwrite:
                                inp = input(f'A cache already exists at {
                                            cache_path}. Are you sure you want to overwrite it? (y/n)')
                                if inp == 'y' or inp == 'Y':
                                    write = True
                            else:
                                write = True

                            if write:
                                print(f"Caching result for {cache_path}.")
                                write_to_postgres(ds, cache_key, overwrite=True)
                        else:
                            raise ValueError("Only delta and postgres backends are implemented for tabular data")

            if filepath_only:
                cache_map = fs.get_mapper(cache_path)
                return cache_map
            else:
                # Do the time series filtering
                if timeseries is not None:
                    match_time = [t for t in tl if t in ds.dims]
                    if len(match_time) == 0:
                        raise RuntimeError(
                            "Timeseries array must return a 'time' dimension for slicing.")

                    time_col = match_time[0]

                    # Assign start and end times if None are passed
                    if start_time is None:
                        start_time = ds[time_col].min().values
                    if end_time is None:
                        end_time = ds[time_col].max().values

                    ds = ds.sel({time_col: slice(start_time, end_time)})

                return ds

        return wrapper
    return create_cacheable
