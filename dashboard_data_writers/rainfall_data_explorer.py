"""Cache tables in postgres for the SPW dashboard."""
import xarray as xr

from sheerwater_benchmarking.utils import cacheable, dask_remote, start_remote
from sheerwater_benchmarking.metrics import get_datasource_fn


@dask_remote
@cacheable(data_type='tabular',
           backend='postgres',
           cache_args=['agg_days', 'grid', 'mask', 'region'])
def rainfall_data(start_time, end_time, agg_days=1,
                  grid='global1_5', mask='lsm', region='global'):
    """Store rainfall data across data sources to the database."""
    # Get the ground truth data
    datasets = []
    for truth in ['era5', 'chirps', 'imerg', 'ghcn']:
        source_fn = get_datasource_fn(truth)
        ds = source_fn(start_time, end_time, 'precip', agg_days=agg_days,
                       grid=grid, mask=mask, region=region)
        ds = ds.rename({'precip': f'{truth}_precip'})
        datasets.append(ds)

    # Merge datasets
    ds = xr.merge(datasets, join='outer')
    ds = ds.drop_vars('spatial_ref')

    # Convert to dataframe
    ds = ds.to_dataframe()
    return ds


if __name__ == "__main__":
    start_remote()
    start_time = '2016-01-01'
    end_time = '2025-02-01'
    agg_days = [1, 7]
    grids = ['global1_5', 'global0_25']
    mask = 'lsm'
    region = 'global'
    for grid in grids:
        for agg_day in agg_days:
            rainfall_data(start_time, end_time, agg_day, grid, mask, region)
            import pdb; pdb.set_trace()
