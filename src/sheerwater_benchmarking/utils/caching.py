from functools import partial
import os
import gcsfs
import xarray as xr
from inspect import signature, Parameter

def cacheable(data_type, immutable_args, timeseries=False):
    def create_cacheable(func):
        def wrapper(*args, **kwargs):
            # Calculate the appropriate cache key
            if 'filepath_only' in kwargs:
                filepath_only = kwargs['filepath_only']
                del kwargs['filepath_only']

            immutable_arg_values = {}
            params = signature(func).parameters
            for a in immutable_args:
                if a in kwargs:
                    immutable_arg_values[a] = kwargs[a]

                for i, p in enumerate(params):
                    if a == p and (params[p].kind == Parameter.VAR_POSITIONAL or params[p].kind == Parameter.POSITIONAL_OR_KEYWORD) and params[p].default == Parameter.empty:
                        immutable_arg_values[a] = args[i]
                    elif a == p and a not in kwargs and params[p].default != Parameter.empty:
                        immutable_arg_values[a] = params[p].default

            imkeys = list(immutable_arg_values.keys())
            imkeys.sort()
            sorted_values = [str(immutable_arg_values[i]) for i in imkeys]

            print(sorted_values)

            if data_type == 'array':
                cache_key = func.__name__ + '/' + '_'.join(sorted_values) + '.zarr'

            print(cache_key)

            cache_path = "gs://sheerwater-datalake/caches/" + cache_key

            # Check to see if the cache exists for this key
            fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
            cache_map = fs.get_mapper(cache_path)
            if fs.exists(cache_path):
                # Read the cache
                print(f"Found cache for {cache_path}")
                if data_type == 'array':
                    if filepath_only:
                        return cache_map
                    else:
                        print(f"Opening cache {cache_path}")
                        ds = xr.open_dataset(cache_map, engine='zarr')

                        # If it's time series check to see if these time keys exists

                        # Return the cache
                        return ds
                else:
                    print("Auto caching currently only supports array types")

            ##### IF NOT EXISTS ######
            print(f"Cache doesn't exist for {cache_path}. Running function")
            ds = func(*args, **kwargs)

            # Store the result
            if data_type == 'array':
                print(f"Autocaching result for {cache_path}.")
                if isinstance(ds, xr.Dataset):
                    ds.to_zarr(store=cache_map, mode='w')

            if filepath_only:
                return cache_map
            else:
                return ds

        return wrapper
    return create_cacheable
