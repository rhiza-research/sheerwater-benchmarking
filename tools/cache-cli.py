import click
from cache import cache_delete, cache_list

@click.group()
def cache():
    pass

@cache.command()
@click.option('--backend', '-b', type=click.Choice(['zarr', 'delta', 'postgres', 'terracotta', 'pickle']), default='zarr', help="The backend to find the cache")
@click.option('--name', '-n', type=str, help="Name of the cache - this is the name of the cacheable function")
@click.option('--glob', '-g', type=str, help="Globable regex of the cache key to delete - pass '*' to delete all caches")
def delete(backend, name, glob):
    num = cache_delete(backend, name, glob)
    if num is not None:
        click.echo(f"Successfully deleted {num} files")

@cache.command()
@click.option('--backend', '-b', type=click.Choice(['zarr', 'delta', 'postgres', 'terracotta', 'pickle']), default='zarr', help="The backend to find the cache")
@click.option('--name', '-n', type=str, help="Name of the cache - this is the name of the cacheable function", required=True)
@click.option('--glob', '-g', type=str, help="Globable regex of the cache key to list - pass '*' to list all caches", required=True)
def list(backend, name, glob):
    files = cache_list(backend, name, glob)
    files = [f + '\n' for f in files]
    files = [f"Found {len(files)} caches:\n"] + files
    click.echo_via_pager(files)
    click.echo(f"Found {len(files)-1} caches.")

@cache.group()
def rename():
    pass

@rename.command('value')
def rename_value():
    pass

@rename.command('cache')
def rename_cache():
    pass

if __name__ == '__main__':
    cache()
