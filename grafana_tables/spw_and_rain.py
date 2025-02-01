"""Cache tables in postgres for the SPW dashboard."""
import xarray as xr

from sheerwater_benchmarking.utils import cacheable, dask_remote, roll_and_agg, apply_mask, clip_region
from sheerwater_benchmarking.metrics import get_datasource_fn
from sheerwater_benchmarking.reanalysis import era5
from sheerwater_benchmarking.baselines import climatology_raw

from sheerwater_benchmarking.tasks import (
    rainy_season_onset_truth, rainy_season_onset_forecast,
)


@dask_remote
@cacheable(data_type='tabular',
           cache_args=['truth', 'grid', 'mask', 'region'],
           backend='postgres')
def rain_windowed_spw(start_time, end_time,
                      truth='era5',
                      grid='global1_5', mask='lsm', region='global'):
    """Store the rolling windows of precipitation relevant to SPW in the database."""
    # Get the ground truth data
    if truth == 'ltn':
        time_dim = 'dayofyear'
        # Call raw climatology data for 12 years prior to evaluation period
        ds = climatology_raw('precip', first_year=2004, last_year=2015, grid=grid)
        # Mask and clip the region
        ds = apply_mask(ds, mask, var='precip', grid=grid)
        ds = clip_region(ds, region=region)
    else:
        time_dim = 'time'
        source_fn = get_datasource_fn(truth)
        if truth == 'ghcn':
            # Call GHCN with non-default mean cell aggregation
            ds = source_fn(start_time, end_time, 'precip', agg_days=1,
                           grid=grid, mask=mask, region=region, cell_aggregation='mean')
        else:
            # Run without GHCN specific cell aggregation flag
            ds = source_fn(start_time, end_time, 'precip', agg_days=1,
                           grid=grid, mask=mask, region=region)

    # Compute the rolling windows of precipitation relevant to SPW
    missing_thresh = 0.5
    agg_days = 8
    agg_thresh = max(int(agg_days*missing_thresh), 1)
    ds['precip_8d'] = roll_and_agg(ds['precip'], agg=agg_days, agg_col=time_dim, agg_fn='sum', agg_thresh=agg_thresh)
    agg_days = 11
    agg_thresh = max(int(agg_days*missing_thresh), 1)
    ds['precip_11d'] = roll_and_agg(ds['precip'], agg=agg_days, agg_col=time_dim, agg_fn='sum', agg_thresh=agg_thresh)

    ds = ds.drop_vars('spatial_ref')
    ds = ds.to_dataframe()
    return ds


@dask_remote
@cacheable(data_type='tabular',
           backend='postgres',
           cache_args=['truth', 'use_ltn', 'grid', 'mask', 'region'])
def ea_rainy_onset_truth(start_time, end_time,
                         truth='era5',
                         use_ltn=False,
                         grid='global1_5', mask='lsm', region='global'):
    """Store the East African rainy season onset from a given truth source, according to the SPW method."""
    ds = rainy_season_onset_truth(start_time, end_time, truth=truth,
                                  use_ltn=use_ltn, first_year=2004, last_year=2015,
                                  groupby=[['ea_rainy_season', 'year']],
                                  region=region, mask=mask, grid=grid)
    if 'spatial_ref' in ds.coords:
        ds = ds.drop_vars('spatial_ref')
    df = ds.to_dataframe()
    df = df.dropna(subset='rainy_onset')
    return df


@dask_remote
@cacheable(data_type='tabular',
           backend='postgres',
           cache_args=['start_time', 'end_time', 'forecast', 'grid', 'mask', 'region'])
def ea_rainy_onset_forecast(start_time, end_time,
                            forecast='ecmwf_ifs_er_debiased',
                            grid='global1_5', mask='lsm', region='global'):
    """Store the rainy season onset from a given forecast source, according to the SPW method."""
    # Get the forecast for rainy season onset
    ds = rainy_season_onset_forecast(start_time, end_time,
                                     forecast=forecast,
                                     region=region, grid=grid, mask=mask)
    ds = ds.drop_vars('spatial_ref')
    df = ds.to_dataframe()
    df = df.dropna(subset='rainy_forecast')
    return df


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
        if truth == 'ghcn':
            # Call GHCN with non-default mean cell aggregation
            ds = source_fn(start_time, end_time, 'precip', agg_days=agg_days,
                           grid=grid, mask=mask, region=region, cell_aggregation='mean')
        else:
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
