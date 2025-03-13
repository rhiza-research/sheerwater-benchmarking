"""Cache tables in postgres for the SPW dashboard."""
from sheerwater_benchmarking.utils import cacheable, dask_remote, apply_mask, clip_region
from sheerwater_benchmarking.reanalysis import era5_rolled
from sheerwater_benchmarking.data.imerg import imerg_rolled
from sheerwater_benchmarking.data.chirps import chirps_rolled
from sheerwater_benchmarking.data.ghcn import _ghcn_rolled_unified
from sheerwater_benchmarking.data.tahmo import tahmo_rolled
from sheerwater_benchmarking.baselines.climatology import climatology_rolled


@dask_remote
@cacheable(data_type='tabular',
           cache_args=['truth', 'grid', 'mask', 'region'],
           backend='postgres')
def daily_tmp2m_prise(start_time, end_time,
                      truth='era5',
                      grid='global1_5', mask='lsm', region='global'):
    """Store the daily precipitation and temperature data in the database."""
    # Get the ground truth data
    if truth == 'era5':
        ds = era5_rolled(start_time, end_time, variable='tmp2m', grid=grid)
    elif truth == 'imerg':
        ds = imerg_rolled(start_time, end_time, grid=grid)
    elif truth == 'chirps':
        ds = chirps_rolled(start_time, end_time, grid=grid)
    elif truth == 'ghcn':
        ds = _ghcn_rolled_unified(start_time, end_time, variable='tmp2m', grid=grid,
                                  missing_thresh=0.0, cell_aggregation='first')
    elif truth == 'tahmo':
        ds = tahmo_rolled(start_time, end_time,
                          grid=grid, missing_thresh=0.0, cell_aggregation='first')
    elif truth == 'ltn':
        ds = climatology_rolled(start_time, end_time, variable='tmp2m',
                                first_year=2004, last_year=2015,
                                prob_type='deterministic', grid=grid)

    ds = apply_mask(ds, mask)
    ds = clip_region(ds, region=region)
    ds = ds.drop_vars('spatial_ref')
    df = ds.to_dataframe().dropna()
    return df
