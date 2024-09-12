"""Automated dataframe caching utilities."""
import dask
import gcsfs
import xarray as xr
import pandas
import dateparser
import datetime
from inspect import signature, Parameter
import logging
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

def prune_chunking_dimensions(ds, chunking):
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
    # Get the chunks for the dataset
    ds_chunks = {dim: ds.chunks[dim][0] for dim in ds.chunks}

    chunking = prune_chunking_dimensions(ds, chunking)

    return ds_chunks == chunking


def drop_encoded_chunks(ds):
    for var in ds.data_vars:
        if 'chunks' in ds[var].encoding:
            del ds[var].encoding['chunks']

    return ds


def cacheable(data_type, cache_args, timeseries=None, chunking=None, auto_rechunk=False):
    # Valid configuration kwargs for the cacheable decorator
    cache_kwargs = {
        "filepath_only": False,
        "recompute": False,
        "cache": True,
        "validate_cache_timeseries": True,
        "force_overwrite": False,
        "retry_null_cache": False,
    }
    """Decorator for caching function results.

    Args:
        data_type (str): The type of data being cached. Currently only 'array' is supported.
        cache_args (list): The arguments to use as the cache key.
        timeseries (str, list): The name of the time series dimension in the cached array. If not a
            time series, set to None (default). If a list, will use the first mactchin coordinate in the list.
        chunking (dict): Specifies chunking if that coordinate exists. If coordinate does not exist
            the chunking specified will be dropped.
        auto_rechunk (bool): If True will aggressively rechunk a cache on load.
        cache (bool): Whether to cache the result.
        validate_cache_timeseries (bool): Whether to validate the cache timeseries against the
            requested timeseries. If False, will not validate the cache timeseries.
        force_overwrite (bool): Whether to overwrite the cache forecable on recompute if it
            already exists (if False, will prompt the user before overwriting).
        retry_null_cache (bool): If True, ignore the null caches and attempts to recompute
            result for null values. If False (default), will return None for null caches.
    """

    def create_cacheable(func):
        def wrapper(*args, **kwargs):
            # Calculate the appropriate cache key
            filepath_only, recompute, cache, validate_cache_timeseries, \
                force_overwrite, retry_null_cache = get_cache_args(
                    kwargs, cache_kwargs)

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

            # Handle keying based on immutable arguments
            immutable_arg_values = {}

            for a in cache_args:
                # If it's in kwargs, great
                if a in kwargs:
                    immutable_arg_values[a] = kwargs[a]
                    continue

                # If it's not in kwargs it must either be (1) in args or (2) passed as default
                found = False
                for i, p in enumerate(params):
                    if (a == p and len(args) > i and
                            (params[p].kind == Parameter.VAR_POSITIONAL or
                             params[p].kind == Parameter.POSITIONAL_OR_KEYWORD)):
                        immutable_arg_values[a] = args[i]
                        found = True
                        break
                    elif a == p and params[p].default != Parameter.empty:
                        immutable_arg_values[a] = params[p].default
                        found = True
                        break

                if not found:
                    raise RuntimeError(f"Specified cacheable argument {a}"
                                       "not discovered as passed argument or default arugment.")

            imkeys = list(immutable_arg_values.keys())
            imkeys.sort()
            sorted_values = [str(immutable_arg_values[i]) for i in imkeys]

            if data_type == 'array':
                cache_key = func.__name__ + '/' + '_'.join(sorted_values) + '.zarr'
                null_key = func.__name__ + '/' + '_'.join(sorted_values) + '.null'
                cache_path = "gs://sheerwater-datalake/caches/" + cache_key
                null_path = "gs://sheerwater-datalake/caches/" + null_key
            else:
                raise ValueError("Caching currently only supports the 'array' datatype")

            # Check to see if the cache exists for this key
            fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
            cache_map = fs.get_mapper(cache_path)

            ds = None
            compute_result = True
            if fs.exists(cache_path) and not recompute and cache:
                # Read the cache
                print(f"Found cache for {cache_path}")
                if data_type == 'array':
                    if filepath_only:
                        return cache_map
                    else:
                        print(f"Opening cache {cache_path}")
                        # We must auto open chunks. This tries to use the underlying zarr chunking if possible.
                        # Setting chunks=True triggers what I think is an xarray/zarr engine bug where
                        # every chunk is only 4B!
                        ds = xr.open_dataset(cache_map, engine='zarr', chunks={})

                        # If rechunk is passed then check to see if the rechunk array matches chunking. If not then rechunk
                        if auto_rechunk:
                            if not isinstance(chunking, dict):
                                raise ValueError(
                                    "If auto_rechunk is True, a chunking dict must be supplied.")

                            # Compare the dict to the rechunk dict
                            if not chunking_compare(ds, chunking):
                                print(
                                    "Rechunk was passed and cached chunks do not match rechunk request. Performing rechunking")

                                # write to a temp cache map
                                temp_cache_path = 'gs://sheerwater-datalake/caches/temp/' + cache_key
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
                                # print("Requested chunks already match rechunk.")
                                pass

                        if validate_cache_timeseries and timeseries is not None:
                            # Check to see if the dataset extends roughly the full time series set
                            match_time = [t for t in tl if t in ds.dims]
                            if len(match_time) == 0:
                                raise RuntimeError("Timeseries array functions must return a time dimension for slicing. "
                                                   "This could be an invalid cache. Try running with recompute=True to reset the cache.")
                            else:
                                time_col = match_time[0]
                                # Check if within 1 year at least
                                if (pandas.Timestamp(ds[time_col].min().values) < dateparser.parse(start_time) + datetime.timedelta(days=365) and
                                        pandas.Timestamp(ds[time_col].max().values) > dateparser.parse(end_time) - datetime.timedelta(days=365)):

                                    compute_result = False
                                else:
                                    print("WARNING: The cached array does not have data within "
                                          "1 year of your start or endtime. Automatically recomputing. "
                                          "If you do not want to recompute the result set "
                                          "`validate_cache_timeseries=False`")
                        else:
                            compute_result = False
                else:
                    print("Auto caching currently only supports array types")
            elif fs.exists(null_path) and not recompute and cache and not retry_null_cache:
                print(f"Found null cache for {null_path}. Skipping computation.")
                return None

            if compute_result:
                if recompute:
                    print(f"Recompute for {cache_path} requested. Not checking for cached result.")
                elif not cache:
                    print(f"{func.__name__} not a cacheable function. Recomputing result.")
                else:
                    print(f"Cache doesn't exist for {cache_path}. Running function")

                ##### IF NOT EXISTS ######
                ds = func(*args, **kwargs)

                # Store the result
                if cache:
                    if ds is None:
                        print(f"Autocaching null result for {null_path}.")
                        with fs.open(null_path, 'wb') as f:
                            f.write(b'')
                            return None
                    elif data_type == 'array':
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
                                if chunking:
                                    # If we aren't doing auto chunking delete the encoding chunks
                                    ds = drop_encoded_chunks(ds)

                                    chunking = prune_chunking_dimension(ds, chunking)

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

            if filepath_only:
                return cache_map
            else:
                # Do the time series filtering
                if timeseries is not None:
                    match_time = [t for t in tl if t in ds.dims]
                    if len(match_time) == 0:
                        raise RuntimeError(
                            "Timeseries array must return a 'time' dimension for slicing.")

                    time_col = match_time[0]
                    ds = ds.sel({time_col: slice(start_time, end_time)})

                return ds

        return wrapper
    return create_cacheable
