"""Automated dataframe caching utilities."""
import gcsfs
import xarray as xr
import pandas
import dateparser
import datetime
from inspect import signature, Parameter


def cacheable(data_type, cache_args, timeseries=False, cache=True):
    """Decorator for caching function results."""
    def create_cacheable(func):
        def wrapper(*args, **kwargs):
            # Calculate the appropriate cache key
            filepath_only = False
            if 'filepath_only' in kwargs:
                filepath_only = kwargs['filepath_only']
                del kwargs['filepath_only']

            recompute = False
            if 'recompute' in kwargs:
                recompute = kwargs['recompute']
                del kwargs['recompute']

            validate_cache_timeseries = True
            if 'validate_cache_timeseries' in kwargs:
                validate_cache_timeseries = kwargs['validate_cache_timeseries']
                del kwargs['validate_cache_timeseries']

            params = signature(func).parameters

            # Validate time series params
            start_time = None
            end_time = None
            if timeseries:
                if 'start_time' in cache_args or 'end_time' in cache_args:
                    print(
                        "ERROR: Time series functions must not place their time arguments in cache_args!")
                    return

                if 'start_time' not in params or 'end_time' not in params:
                    print(
                        "ERROR: Time series functions must have the parameters 'start_time' and 'end_time'")
                else:
                    keys = [item for item in params]
                    start_time = args[keys.index('start_time')]
                    end_time = args[keys.index('end_time')]

            # Handle keying based on immutable arguments
            immutable_arg_values = {}
            for a in cache_args:
                if a in kwargs:
                    immutable_arg_values[a] = kwargs[a]
                    continue

                for i, p in enumerate(params):
                    if (a == p and len(args) > i and
                            (params[p].kind == Parameter.VAR_POSITIONAL or
                             params[p].kind == Parameter.POSITIONAL_OR_KEYWORD)):
                        immutable_arg_values[a] = args[i]
                        break
                    elif a == p and params[p].default != Parameter.empty:
                        immutable_arg_values[a] = params[p].default
                        break

            imkeys = list(immutable_arg_values.keys())
            imkeys.sort()
            sorted_values = [str(immutable_arg_values[i]) for i in imkeys]

            if data_type == 'array':
                cache_key = func.__name__ + '/' + \
                    '_'.join(sorted_values) + '.zarr'
                null_key = func.__name__ + '/' + \
                    '_'.join(sorted_values) + '.null'
                cache_path = "gs://sheerwater-datalake/caches/" + cache_key
                null_path = "gs://sheerwater-datalake/caches/" + null_key

            # Check to see if the cache exists for this key
            fs = gcsfs.GCSFileSystem(
                project='sheerwater', token='google_default')
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
                        ds = xr.open_dataset(cache_map, engine='zarr')

                        if validate_cache_timeseries and timeseries:
                            # Check to see if the dataset extends roughly the full time series set
                            if 'time' not in ds.dims:
                                print(
                                    "ERROR: Timeseries array must return a 'time' dimension for slicing. "
                                    "This could be an invalid cache. Try running with `recompute=True` to reset the cache.")
                                return
                            else:
                                # Check if within 1 year at least
                                if (pandas.Timestamp(ds.time.min().values) < dateparser.parse(start_time) + datetime.timedelta(days=365) and
                                        pandas.Timestamp(ds.time.max().values) > dateparser.parse(end_time) - datetime.timedelta(days=365)):

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
            elif fs.exists(null_path) and not recompute and cache:
                print(f"Found null cache for {
                      null_path}. Skipping computation.")
                return None

            if compute_result:
                if recompute:
                    print(f"Recompute for {
                          cache_path} requested. Not checking for cached result.")
                elif not cache:
                    print(
                        f"{func.__name__} not a cacheable function. Recomputing result.")
                else:
                    print(f"Cache doesn't exist for {
                          cache_path}. Running function")

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
                        print(f"Autocaching result for {cache_path}.")
                        write = False
                        if fs.exists(cache_path):
                            inp = input(f'A cache already exists at {cache_path}. Are you sure you want to overwrite it? (y/n)')
                            if inp == 'y' or inp == 'Y':
                                write = True
                        else:
                            write = True

                        if write:
                            print(f"Caching result for {cache_path}.")
                            if isinstance(ds, xr.Dataset):
                                ds.chunk(chunks="auto").to_zarr(store=cache_map, mode='w')
                            else:
                                raise RuntimeError(f"Array datatypes must return xarray datasets or None instead of {type(ds)}")

            if filepath_only:
                return cache_map
            else:
                # Do the time series filtering
                if timeseries:
                    if 'time' not in ds.dims:
                        print(
                            "ERROR: Timeseries array must return a 'time' dimension for slicing.")
                        return

                    ds = ds.sel(time=slice(start_time, end_time))

                return ds

        return wrapper
    return create_cacheable
