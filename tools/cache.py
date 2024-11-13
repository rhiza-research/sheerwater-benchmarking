import gcsfs
import click
from sheerwater_benchmarking.utils.secrets import postgres_read_password, postgres_write_password
import terracotta as tc
import re

def cache_list(backend, name, glob):
    if backend in ['zarr', 'delta', 'pickle']:
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
    else:
        raise ValueError("Unsupported backend.")

def cache_delete(backend, name, glob):

    to_delete = cache_list(backend, name, glob)

    if backend in ['zarr', 'delta', 'pickle']:
        fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')

        click.echo(to_delete)
        if click.confirm("Do you want to delete these caches?"):
            for i, f in enumerate(to_delete):
                fs.rm(f, recursive=True)

        return len(to_delete)
    elif backend == 'terracotta':
        tc.update_settings(SQL_USER="write", SQL_PASSWORD=postgres_write_password())
        driver = tc.get_driver("postgresql://sheerwater-benchmarking-postgres:5432/terracotta")

        click.echo(to_delete)
        if click.confirm("Do you want to delete these caches?"):

            del_map = {'key': f for f in to_delete}
            driver.delete(del_map)

        return len(to_delete)
    else:
        pass


    print("Files successfully deleted!")
