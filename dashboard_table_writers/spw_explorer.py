"""Cache tables in postgres for the SPW dashboard."""
import xarray as xr

from sheerwater_benchmarking.utils import cacheable, dask_remote, apply_mask, clip_region
from sheerwater_benchmarking.reanalysis import era5_rolled
from sheerwater_benchmarking.baselines import climatology_agg_raw
from sheerwater_benchmarking.reanalysis.era5 import era5_spw
from sheerwater_benchmarking.forecasts.ecmwf_er import ifs_extended_range_spw


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
        # Get the rolled and aggregated data, and then multiply average daily precip by the number of days
        datasets = [agg_days*climatology_agg_raw('precip', first_year=2004, last_year=2015,
                                                 prob_type='deterministic',
                                                 agg_days=agg_days, grid=grid)
                    .rename({'precip': f'precip_{agg_days}d'})
                    for agg_days in [8, 11]]
    else:
        if truth == 'era5':
            datasets = [agg_days*era5_rolled(start_time, end_time, 'precip',  agg_days=agg_days, grid=grid)
                        .rename({'precip': f'precip_{agg_days}d'})
                        for agg_days in [8, 11]]
        else:
            raise NotImplementedError(f"Truth source {truth} not implemented.")

    # Merge both datasets
    ds = xr.merge(datasets)

    # Apply masking
    ds = apply_mask(ds, mask, grid=grid)
    ds = clip_region(ds, region=region)
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
    if truth == 'era5':
        ds = era5_spw(start_time, end_time,
                      region=region, mask=mask, grid=grid,
                      use_ltn=use_ltn, first_year=2004, last_year=2015,
                      groupby=['ea_rainy_season', 'year'])
    else:
        raise NotImplementedError(f"Truth source {truth} not implemented.")

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
    if forecast == 'ecmwf_ifs_er_debiased' or forecast == 'ecmwf_ifs_er':
        debiased = (forecast == 'ecmwf_ifs_er_debiased')
        datasets = []
        for lead in [f'day{d}' for d in range(1, 28)]:
            ds = ifs_extended_range_spw(start_time, end_time, lead,
                                        prob_type='probabilistic',
                                        prob_threshold=None,
                                        region=region, mask=mask, grid=grid,
                                        groupby=None,
                                        debiased=debiased)
            datasets.append(ds)

        ds = xr.concat(datasets, dim='lead_time')
        ds = ds.drop_vars('spatial_ref')
    else:
        raise NotImplementedError(f"Forecast source {forecast} not implemented.")
    df = ds.to_dataframe()
    df = df.dropna(subset='rainy_forecast')
    return df


if __name__ == "__main__":
    # Runners to generate the tables
    start_time = '2016-01-01'
    end_time = '2022-12-31'

    forecast = 'ecmwf_ifs_er_debiased'
    truth = 'era5'
    use_ltn = False
    grid = 'global1_5'
    mask = 'lsm'
    region = 'global'

    ea_rainy_onset_truth(start_time, end_time, truth, use_ltn, grid, mask, region)
    ea_rainy_onset_forecast(start_time, end_time, forecast, grid, mask, region)
    rain_windowed_spw(start_time, end_time, truth, grid, mask, region)
