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
    truth = ['era5', 'chirps', 'imerg', 'ghcn']
    if region in ['africa', 'east_africa']:
        truth.append('tahmo')
    for truth in truth:
        source_fn = get_datasource_fn(truth)
        ds = source_fn(start_time, end_time, 'precip', agg_days=agg_days,
                       grid=grid, mask=mask, region=region)
        ds = ds.rename({'precip': f'{truth}_precip'})
        datasets.append(ds)

    # Merge datasets
    ds = xr.merge(datasets, join='outer')
    ds = ds.drop_vars(['number', 'spatial_ref'])

    # Convert to dataframe
    df = ds.to_dataframe()
    return df


if __name__ == "__main__":
    start_remote()
    start_time = '2022-01-01'
    end_time = '2024-12-31'
    agg_days = [1, 7]
    for agg_day in agg_days:
        ds = rainfall_data(start_time, end_time, agg_day, grid='global0_25', mask='lsm', region='africa')
