from functools import partial
import os
import gcsfs
import xarray as xr
from inspect import signature, Parameter

def cacheable(data_type, immutable_args, timeseries=False, cache=True):
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
                del kwargs['valide_cache_timeseries']

            params = signature(func).parameters


            # Validate time series params
            start_time = None
            end_time = None
            if timeseries:
                if 'start_time' in immutable_args or 'end_time' in immutable_args:
                    print("ERROR: Time series functions must not place their time arguments in immutable_args!")
                    return

                if 'start_time' not in params or 'end_time' not in params:
                    print("ERROR: Time series functions must have the parameters 'start_time' and 'end_time'")
                else:
                    keys = [item for item in params]
                    start_time = args[keys.index('start_time')]
                    end_time = args[keys.index('end_time')]

            # Handle keying based on immutable arguments
            immutable_arg_values = {}
            for a in immutable_args:
                if a in kwargs:
                    immutable_arg_values[a] = kwargs[a]
                    continue

                for i, p in enumerate(params):
                    if a == p and (params[p].kind == Parameter.VAR_POSITIONAL or params[p].kind == Parameter.POSITIONAL_OR_KEYWORD) and params[p].default == Parameter.empty:
                        immutable_arg_values[a] = args[i]
                    elif a == p and a not in kwargs and params[p].default != Parameter.empty:
                        immutable_arg_values[a] = params[p].default

            imkeys = list(immutable_arg_values.keys())
            imkeys.sort()
            sorted_values = [str(immutable_arg_values[i]) for i in imkeys]

            if data_type == 'array':
                cache_key = func.__name__ + '/' + '_'.join(sorted_values) + '.zarr'

            cache_path = "gs://sheerwater-datalake/caches/" + cache_key

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
                        compute_result = False
                        return cache_map
                    else:
                        print(f"Opening cache {cache_path}")
                        ds = xr.open_dataset(cache_map, engine='zarr')

                        #if validate_cache_timeries and timeseries:
                        #    # Check to see if the dataset extends roughly the full time series set
                        #    if 'time' not in ds.dims:
                        #        print("ERROR: Timeseries array must return a 'time' dimension for slicing. This could be an invalid cache. Try running with `recompute=True` to reset the cache.")
                        #        return
                        #    else:
                        #        if ds.time.


                        compute_result = False
                else:
                    print("Auto caching currently only supports array types")

            if compute_result:
                if recompute:
                    print("Recompute requested. Not checking for cached result.")
                elif not cache:
                    print("Not a cacheable function. Recomputing result.")
                else:
                    print(f"Cache doesn't exist for {cache_path}. Running function")

                ##### IF NOT EXISTS ######
                ds = func(*args, **kwargs)

                # Store the result
                if cache:
                    if data_type == 'array':
                        print(f"Autocaching result for {cache_path}.")
                        if isinstance(ds, xr.Dataset):
                            ds.chunk(chunks="auto").to_zarr(store=cache_map, mode='w')

            if filepath_only:
                return cache_map
            else:
                # Do the time series filtering
                if timeseries:
                    if 'time' not in ds.dims:
                        print("ERROR: Timeseries array must return a 'time' dimension for slicing.")
                        return

                    ds = ds.sel(time=slice(start_time, end_time))

                return ds

        return wrapper
    return create_cacheable
