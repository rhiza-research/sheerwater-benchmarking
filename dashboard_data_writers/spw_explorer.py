"""Cache tables in postgres for the SPW dashboard."""
import xarray as xr
import numpy as np
from functools import partial
from sheerwater_benchmarking.utils import cacheable, dask_remote, start_remote
from sheerwater_benchmarking.reanalysis import era5_rolled
from sheerwater_benchmarking.forecasts.ecmwf_er import ecmwf_ifs_spw
from sheerwater_benchmarking.forecasts.fuxi import fuxi_spw
from sheerwater_benchmarking.climatology import climatology_spw, climatology_rolled
from sheerwater_benchmarking.data.imerg import imerg_rolled
from sheerwater_benchmarking.data.chirps import chirps_rolled
from sheerwater_benchmarking.data.ghcn import _ghcn_rolled_unified
from sheerwater_benchmarking.data.tahmo import tahmo_rolled
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
           cache_args=['truth', 'variable', 'grid', 'mask', 'region'])
def ea_rainy_onset_truth(start_time, end_time,
                         truth='era5',
                         variable='rainy_onset',
                         grid='global1_5', mask='lsm', region='global'):
    """Store the East African rainy season onset from a given truth source, according to the SPW method."""
    fn = get_datasource_fn(truth)
    kwargs = {'missing_thresh': 0.0} if truth in ['tahmo', 'ghcn'] else {}
    ds = fn(start_time, end_time, variable, agg_days=None,
            grid=grid, mask=mask, region=region, **kwargs)
    if 'spatial_ref' in ds.coords:
        ds = ds.drop_vars('spatial_ref')
    df = ds.to_dataframe()
    df = df.dropna(subset=variable)
    return df


@dask_remote
@cacheable(data_type='tabular',
           backend='postgres',
           cache_args=['forecast', 'variable', 'grid', 'mask', 'region'])
def ea_rainy_onset_forecast(start_time, end_time,
                            forecast='ecmwf_ifs_er_debiased',
                            variable='rainy_onset',
                            grid='global1_5', mask='lsm', region='global'):
    """Store the rainy season onset from a given forecast source, according to the SPW method."""
    fn = get_datasource_fn(forecast)
    if forecast in ['ecmwf_ifs_er_debiased', 'ecmwf_ifs_er', 'fuxi']:
        if variable == 'rainy_onset_no_drought':  # 20 days instead of 21 to enable FuXi, which only goes out to 42 days
            leads = [f'day{d}' for d in [1, 7, 14, 20]]
        else:
            leads = [f'day{d}' for d in [1, 7, 14, 20, 28]]
    else:
        leads = ['day1']

    datasets = []
    for lead in leads:
        data = fn(start_time, end_time, variable, lead=lead,
                  grid=grid, mask=mask, region=region)
        datasets.append(data)

    ds = xr.concat(datasets, dim='lead_time')
    # if forecast == 'climatology_2015':
    # ds = ds.assign_coords(lead_time=[np.timedelta64(0, 'D')])
    df = ds.to_dataframe()
    df = df.dropna(subset=variable)
    return df


@dask_remote
@cacheable(data_type='tabular',
           backend='postgres',
           cache_args=['forecast', 'variable', 'grid', 'mask', 'region'])
def ea_rainy_onset_probabilities(start_time, end_time,
                                 forecast='ecmwf_ifs_er_debiased',
                                 variable='rainy_onset',
                                 grid='global1_5', mask='lsm', region='global'):
    """Store the rainy season onset probabilities from a given forecast source, according to the SPW method."""
    datasets = []
    drought_condition = (variable == 'rainy_onset_no_drought')
    if forecast in ['ecmwf_ifs_er_debiased', 'ecmwf_ifs_er', 'fuxi']:
        if drought_condition:  # 20 days instead of 21 to enable FuXi, which only goes out to 42 days
            leads = [f'day{d}' for d in [1, 7, 14, 20]]
        else:
            leads = [f'day{d}' for d in [1, 7, 14, 20, 28]]
    else:
        leads = ['day1']

    for lead in leads:
        if forecast in ['ecmwf_ifs_er_debiased', 'ecmwf_ifs_er']:
            ds = ecmwf_ifs_spw(start_time, end_time, lead=lead,
                               debiased=(forecast == 'ecmwf_ifs_er_debiased'),
                               onset_group=None, drought_condition=drought_condition,
                               prob_type='probabilistic', prob_threshold=None,
                               grid=grid, mask=mask, region=region)
        elif forecast == 'fuxi':
            ds = fuxi_spw(start_time, end_time, lead=lead,
                          onset_group=None, drought_condition=drought_condition,
                          prob_type='probabilistic', prob_threshold=None,
                          grid=grid, mask=mask, region=region)
        elif forecast == 'climatology_2015':
            ds = climatology_spw(start_time, end_time, first_year=2004, last_year=2015,
                                 onset_group=None, drought_condition=drought_condition,
                                 prob_type='probabilistic', prob_threshold=None,
                                 grid=grid, mask=mask, region=region)
        else:
            raise ValueError(f"Invalid forecast source: {forecast}")
        datasets.append(ds)
    ds = xr.concat(datasets, dim='lead_time')
    if forecast == 'climatology_2015':
        ds = ds.assign_coords(lead_time=[np.timedelta64(0, 'D')])
    df = ds.to_dataframe()
    df = df.dropna(subset=variable)
    return df


if __name__ == "__main__":
    start_remote(remote_config='xlarge_cluster')
    # Runners to generate the tables
    start_time = '2016-01-01'
    end_time = '2022-12-31'
    grid = 'global1_5'
    mask = 'lsm'
    region = 'kenya'
    variables = ['rainy_onset_no_drought', 'rainy_onset']

    for variable in variables:
        for forecast in ['ecmwf_ifs_er_debiased', 'ecmwf_ifs_er', 'climatology_2015', 'fuxi']:
            df = ea_rainy_onset_forecast(start_time, end_time, forecast,
                                         variable=variable,
                                         grid=grid, mask=mask, region=region,
                                         #  recompute=True, force_overwrite=True,
                                         backend='postgres')

            df = ea_rainy_onset_probabilities(start_time, end_time, forecast,
                                              variable=variable,
                                              grid=grid, mask=mask, region=region,
                                              #   recompute=True, force_overwrite=True,
                                              backend='postgres')

        # Generate for all data sources
        for truth in ["era5", "chirps", "ghcn", "imerg", "tahmo"]:
            df2 = ea_rainy_onset_truth(start_time, end_time, truth,
                                       variable=variable,
                                       grid=grid, mask=mask, region=region,
                                       #    recompute=True,
                                       #    force_overwrite=True,
                                       backend='postgres')
            # df3 = rain_windowed_spw(start_time, end_time, truth,
            # grid=grid, mask=mask, region=region,
            # backend='postgres', recompute=True, force_overwrite=True)
