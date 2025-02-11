"""Cache tables in postgres for the SPW dashboard."""
import xarray as xr
import numpy as np
from functools import partial
from sheerwater_benchmarking.utils import cacheable, dask_remote, start_remote
from sheerwater_benchmarking.reanalysis import era5_rolled
from sheerwater_benchmarking.forecasts.ecmwf_er import ecmwf_ifs_spw
from sheerwater_benchmarking.baselines.climatology import climatology_spw
from sheerwater_benchmarking.data.imerg import imerg_rolled
from sheerwater_benchmarking.data.chirps import chirps_rolled
from sheerwater_benchmarking.data.ghcn import _ghcn_rolled_unified
from sheerwater_benchmarking.data.tahmo import tahmo_rolled
from sheerwater_benchmarking.baselines.climatology import climatology_rolled
from sheerwater_benchmarking.metrics import get_datasource_fn
from sheerwater_benchmarking.tasks import spw_precip_preprocess


@dask_remote
@cacheable(data_type='tabular',
           cache_args=['truth', 'grid', 'mask', 'region'],
           backend='postgres')
def rain_windowed_spw(start_time, end_time,
                      truth='era5',
                      grid='global1_5', mask='lsm', region='global'):
    """Store the rolling windows of precipitation relevant to SPW in the database."""
    # Get the ground truth data
    if truth == 'era5':
        fn = partial(era5_rolled, start_time, end_time, variable='precip', grid=grid)
    elif truth == 'imerg':
        fn = partial(imerg_rolled, start_time, end_time, grid=grid)
    elif truth == 'chirps':
        fn = partial(chirps_rolled, start_time, end_time, grid=grid)
    elif truth == 'ghcn':
        fn = partial(_ghcn_rolled_unified, start_time, end_time, variable='precip', grid=grid,
                     missing_thresh=0.0, cell_aggregation='first')
    elif truth == 'tahmo':
        fn = partial(tahmo_rolled, start_time, end_time,
                     grid=grid, missing_thresh=0.0, cell_aggregation='first')
    elif truth == 'ltn':
        fn = partial(climatology_rolled, start_time, end_time, variable='precip',
                     first_year=2004, last_year=2015,
                     prob_type='deterministic', grid=grid)
    ds = spw_precip_preprocess(fn, mask=mask, region=region, grid=grid,
                               agg_days=[1, 8, 11], shift_days=[0, 0, 0])
    ds = ds.drop_vars('spatial_ref')
    df = ds.to_dataframe().dropna()
    return df


@dask_remote
@cacheable(data_type='tabular',
           backend='postgres',
           cache_args=['truth', 'use_ltn', 'grid', 'mask', 'region'])
def ea_rainy_onset_truth(start_time, end_time,
                         truth='era5',
                         use_ltn=False,  # noqa: ARG001
                         grid='global1_5', mask='lsm', region='global'):
    """Store the East African rainy season onset from a given truth source, according to the SPW method."""
    fn = get_datasource_fn(truth)
    kwargs = {'missing_thresh': 0.0} if truth in ['tahmo', 'ghcn'] else {}
    ds = fn(start_time, end_time, 'rainy_onset', agg_days=None,
            grid=grid, mask=mask, region=region, **kwargs)
    if 'spatial_ref' in ds.coords:
        ds = ds.drop_vars('spatial_ref')
    df = ds.to_dataframe()
    df = df.dropna(subset='rainy_onset')
    return df


@dask_remote
@cacheable(data_type='tabular',
           backend='postgres',
           cache_args=['forecast', 'use_ltn', 'grid', 'mask', 'region'])
def ea_rainy_onset_forecast(start_time, end_time,
                            forecast='ecmwf_ifs_er_debiased',
                            use_ltn=False,  # noqa: ARG001
                            grid='global1_5', mask='lsm', region='global'):
    """Store the rainy season onset from a given forecast source, according to the SPW method."""
    fn = get_datasource_fn(forecast)
    datasets = []
    if forecast in ['ecmwf_ifs_er_debiased', 'ecmwf_ifs_er']:
        leads = [f'day{d}' for d in [1, 7, 14, 21, 28]]
    else:
        leads = ['day1']

    for lead in leads:
        ds = fn(start_time, end_time, 'rainy_onset', lead=lead,
                grid=grid, mask=mask, region=region)
    datasets.append(ds)
    ds = xr.concat(datasets, dim='lead_time')
    if forecast == 'climatology_2015':
        ds = ds.assign_coords(lead_time=[np.timedelta64(0, 'D')])
    df = ds.to_dataframe()
    df = df.dropna(subset='rainy_onset')
    return df


@dask_remote
@cacheable(data_type='tabular',
           backend='postgres',
           cache_args=['forecast', 'use_ltn', 'grid', 'mask', 'region'])
def ea_rainy_onset_probabilities(start_time, end_time,
                            forecast='ecmwf_ifs_er_debiased',
                            use_ltn=False,  # noqa: ARG001
                            grid='global1_5', mask='lsm', region='global'):
    """Store the rainy season onset probabilities from a given forecast source, according to the SPW method."""
    datasets = []
    if forecast in ['ecmwf_ifs_er_debiased', 'ecmwf_ifs_er']:
        leads = [f'day{d}' for d in [1, 7, 14, 21, 28]]
    else:
        leads = ['day1']

    for lead in leads:
        if forecast in ['ecmwf_ifs_er_debiased', 'ecmwf_ifs_er']:
            ds = ecmwf_ifs_spw(start_time, end_time, lead=lead,
                               debiased=(forecast == 'ecmwf_ifs_er_debiased'),
                               onset_group=None,
                               prob_type='probabilistic', prob_threshold=None,
                               grid=grid, mask=mask, region=region)
        elif forecast == 'climatology_2015':
            ds = climatology_spw(start_time, end_time, first_year=2004, last_year=2015,
                                 onset_group=None,
                                 prob_type='probabilistic', prob_threshold=None,
                                 grid=grid, mask=mask, region=region)
        else:
            raise ValueError(f"Invalid forecast source: {forecast}")
        datasets.append(ds)
    ds = xr.concat(datasets, dim='lead_time')
    if forecast == 'climatology_2015':
        ds = ds.assign_coords(lead_time=[np.timedelta64(0, 'D')])

    df = ds.to_dataframe()
    df = df.dropna(subset='rainy_onset')
    return df


if __name__ == "__main__":
    start_remote()
    # Runners to generate the tables
    start_time = '2016-01-01'
    end_time = '2022-12-31'
    grid = 'global1_5'
    mask = 'lsm'
    region = 'kenya'

    for forecast in ['ecmwf_ifs_er_debiased', 'ecmwf_ifs_er', 'climatology_2015']:
        df = ea_rainy_onset_forecast(start_time, end_time, forecast,
                                     grid=grid, mask=mask, region=region,
                                     recompute=True,
                                     backend='postgres')

        df = ea_rainy_onset_probabilities(start_time, end_time, forecast,
                                          grid=grid, mask=mask, region=region,
                                          recompute=True,
                                          backend='postgres')

    # Generate for all data sources
    for truth in ["era5", "chirps", "ghcn", "imerg", "tahmo", 'ltn']:
        df2 = ea_rainy_onset_truth(start_time, end_time, truth,
                                   grid=grid, mask=mask, region=region,
                                   backend='postgres', recompute=True, force_overwrite=True)
        df3 = rain_windowed_spw(start_time, end_time, truth, grid, mask, region,
                                backend='postgres', recompute=True, force_overwrite=True)
