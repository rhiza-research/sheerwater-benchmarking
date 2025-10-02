"""Utility functions for caching and deleting caches."""
import gcsfs
import click
import terracotta as tc
import re
import sqlalchemy
from sqlalchemy import text
import xarray as xr
import pandas as pd

from sheerwater_benchmarking.utils import dask_remote
from sheerwater_benchmarking.utils.secrets import postgres_write_password
from sheerwater_benchmarking.utils.caching import read_from_postgres


@dask_remote
def cache_list_null(name, glob, null_frac=0.0):
    """List all the caches that have a fraction of null values above a certain threshold.

    NOTE: this is a slow function for big dataframes and should be run on a sufficient Dask cluster.

    Args:
        name (str): The name of the cache.
        glob (str): The glob pattern to search for.
        null_frac (float): The fraction of null values to check for.
    """
    print("Warning: This function is slow for big zarrs and should be run on a sufficient Dask cluster.")
    files = cache_list('zarr', name, glob)
    null_files = []
    for f in files:
        ds = xr.open_zarr('gs://' + f)
        # Check if ds is above a fraction of null values
        null_count = float(ds.isnull().sum().compute().to_array().values[0])
        data_count = float(ds.count().compute().to_array().values[0])

        if data_count == 0:
            print(f"WARNING: gs://{f} has no data.")
            null_files.append(f)
            continue
        if (null_count / data_count) > null_frac:
            print(f"WARNING: {null_count / data_count * 100}% missing values,\n\tgs://{f}.")
            null_files.append(f)

    return null_files


@dask_remote
def cache_delete_null(name, glob, null_frac=0.0):
    """List all the caches that have a fraction of null values above a certain threshold.

    NOTE: this is a slow function for big dataframes and should be run on a sufficient Dask cluster.

    Args:
        name (str): The name of the cache.
        glob (str): The glob pattern to search for.
        null_frac (float): The fraction of null values to check for.
    """
    null_files = cache_list_null(name, glob, null_frac)
    return _gui_cache_delete(null_files, backend='zarr')


def cache_list(backend, name, glob):
    """List all the caches that match the given name and glob pattern.

    Args:
        backend (str): The backend to use. One of: 'zarr', 'delta', 'pkl', 'terracotta', 'postgres'.
        name (str): The name of the cache.
        glob (str): The glob pattern to search for.
    """
    if backend in ['zarr', 'delta', 'pkl', 'parquet']:
        fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')

        name = name.rstrip('/')

        # Check if the cache name exists
        cache_name = "gs://sheerwater-datalake/caches/" + name + '/'

        glob = glob.lstrip('/')

        if glob == '*':
            full_glob = cache_name + '*'
            return fs.glob(full_glob)
        else:
            glob = glob.rstrip('*').lstrip('*')
            full_glob = cache_name + '*' + glob + '*'
            return fs.glob(full_glob)
    elif backend == 'terracotta':
        tc.update_settings(SQL_USER="write", SQL_PASSWORD=postgres_write_password())
        driver = tc.get_driver("postgresql://sheerwater-benchmarking-postgres:5432/terracotta")

        ds = driver.get_datasets()

        if glob == '*':
            full_glob = name + '*'
        else:
            glob = glob.rstrip('*').lstrip('*')
            full_glob = name + '*' + glob + '*'

        full_glob = full_glob.replace('*', '.*')

        keys_to_list = []
        for key in ds:
            key = key[0]
            if key.startswith(name) and re.search(full_glob, key):
                keys_to_list.append(key)

        return keys_to_list
    elif backend == 'postgres':
        # Get the cache names table
        df = read_from_postgres('cache_tables', hash_table_name=False)

        if glob == '*':
            full_glob = name + '*'
        else:
            glob = glob.rstrip('*').lstrip('*')
            full_glob = name + '*' + glob + '*'

        full_glob = full_glob.replace('*', '.*')

        # list and filter the keys
        df = df[df.table_name.str.contains(full_glob)]

        return list(df.table_name)

    else:
        raise ValueError("Unsupported backend.")


def cache_delete(backend, name, glob):
    """Delete all the caches that match the given name and glob pattern.

    Args:
        backend (str): The backend to use. One of: 'zarr', 'delta', 'pkl', 'terracotta', 'postgres'.
        name (str): The name of the cache.
        glob (str): The glob pattern to search for.
    """
    to_delete = cache_list(backend, name, glob)
    return _gui_cache_delete(to_delete, backend)



def _gui_cache_delete(to_delete, backend):
    """Delete all the caches in a given list and return the number of caches deleted."""
    if len(to_delete) == 0:
        click.echo("No files to delete.")
        return

    if backend in ['zarr', 'delta', 'pkl']:
        fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')

        click.echo(to_delete)
        length = len(to_delete)
        if click.confirm("Do you want to delete these caches?"):
            for i, f in enumerate(to_delete):
                click.echo(f"Deleting {i+1}/{length}")
                fs.rm(f, recursive=True)

        return len(to_delete)
    elif backend == 'terracotta':
        tc.update_settings(SQL_USER="write", SQL_PASSWORD=postgres_write_password())
        driver = tc.get_driver("postgresql://sheerwater-benchmarking-postgres:5432/terracotta")

        click.echo(to_delete)

        length = len(to_delete)
        if click.confirm("Do you want to delete these caches?"):
            for i, f in enumerate(to_delete):
                click.echo(f"Deleting {i+1}/{length}")
                del_map = {'key': f}
                driver.delete(del_map)

        return len(to_delete)
    elif backend == 'postgres':
        df = read_from_postgres('cache_tables', hash_table_name=False)

        # Filter for the to delete values
        df = df[df.table_name.isin(to_delete)]

        # Find the cache keys
        keys_to_delete = list(df.table_key)

        # Delete the cache keys
        pgwrite_pass = postgres_write_password()
        engine = sqlalchemy.create_engine(
            f'postgresql://write:{pgwrite_pass}@sheerwater-benchmarking-postgres:5432/postgres')

        click.echo(to_delete)
        ls = len(keys_to_delete)
        if click.confirm("Do you want to delete these caches?"):
            with engine.connect() as connection:
                for i, key in enumerate(keys_to_delete):
                    click.echo(f"Deleting {i+1}/{ls}")
                    connection.execute(text(f'DROP Table IF EXISTS "{key}"'))
                    connection.execute(text(f"DELETE from cache_tables WHERE table_key = '{key}'"))

                connection.commit()

        return len(keys_to_delete)
    else:
        pass

    print("Files successfully deleted!")


def cache_verify(backend, name, glob):
    """Delete all the caches that match the given name and glob pattern.

    Args:
        backend (str): The backend to use. One of: 'zarr', 'delta', 'pkl', 'terracotta', 'postgres'.
        name (str): The name of the cache.
        glob (str): The glob pattern to search for.
    """
    to_verify = cache_list(backend, name, glob)

    verified = 0
    for f in to_verify:
        base = '.'.join(f.split('.')[:-1])
        end = f.split('.')[-1]
        verify = base + '.verify'

        if end == backend:
            fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
            fs.touch(verify)
            verified += 1

    return verified

def rename_postgres(old_name, new_name):
    pgwrite_pass = postgres_write_password()

    POSTGRES_IP = "34.59.163.82"

    uri = f'postgresql://write:{pgwrite_pass}@{POSTGRES_IP}:5432/postgres'
    engine = sqlalchemy.create_engine(uri)

    with engine.begin() as conn:
        print(f"Renaming {old_name} to {new_name}.")
        try:
            query_rename = f'ALTER TABLE "{old_name}" RENAME TO "{new_name}"'
            conn.exec_driver_sql(query_rename)

            query_delete = f"DELETE FROM cache_tables where table_name = '{old_name}'"
            conn.exec_driver_sql(query_delete)
        except Exception as e:
            if 'already exists' in str(e):
                pass

        # Now update the renaming entry
        pd_name = {'table_name': [new_name], 'table_key': [new_name], 'created_at': [pd.Timestamp.now()]}
        pd_name = pd.DataFrame(pd_name)
        pd_name.to_sql('cache_tables', engine, if_exists='append')


def internal_rename(f, backend, old_name, new_name):
    if backend == 'postgres':
        rename_to = f.replace(old_name, new_name)
        rename_postgres(f, rename_to)
    elif backend in ['zarr', 'parquet', 'delta', 'pkl']:
        end = f.split('.')[-1]
        if end == backend:
            rename_to = f.replace(old_name, new_name)
            fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
            fs.cp(f, rename_to, recursive=True)


def cache_rename(backend, old_name, new_name, glob, parallel):
    """Delete all the caches that match the given name and glob pattern.

    Args:
        backend (str): The backend to use. One of: 'zarr', 'delta', 'pkl', 'terracotta', 'postgres'.
        name (str): The name of the cache.
        glob (str): The glob pattern to search for.
    """
    to_rename = cache_list(backend, old_name, glob)


    if parallel:
        import multiprocessing
        from functools import partial
        with multiprocessing.Pool() as pool:
            r = pool.map(partial(internal_rename, backend=backend, old_name=old_name, new_name=new_name), to_rename)
    else:
        for f in to_rename:
            internal_rename(f, backend, old_name, new_name)

    return True
