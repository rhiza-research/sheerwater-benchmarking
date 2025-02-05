"""Pulls Salient Predictions S2S forecasts from the Salient API."""
import numpy as np
import xarray as xr

from sheerwater_benchmarking.utils import (cacheable, dask_remote, get_variable, apply_mask, clip_region, regrid,
                                           target_date_to_forecast_date, shift_forecast_date_to_target_date)
from sheerwater_benchmarking.tasks.spw import spw_rainy_onset


@dask_remote
def salient_blend_raw(variable, timescale="sub-seasonal"):
    """Salient function that returns data from GCP mirror.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        timescale (str): The timescale of the forecast. One of:
            - sub-seasonal
            - seasonal
            - long-range

    """
    # Pull the Salient dataset
    var = get_variable(variable, 'salient')
    filename = f'gs://sheerwater-datalake/salient-data/v9/africa/{var}_{timescale}/blend'
    ds = xr.open_zarr(filename,
                      chunks={'forecast_date': 3, 'lat': 300, 'lon': 316,
                              'lead': 10, 'quantile': 23, 'model': 5})
    ds = ds['vals'].to_dataset()
    ds = ds.rename(vals=variable)
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='forecast_date',
           cache_args=['variable', 'timescale', 'grid'],
           chunking={"lat": 721, "lon": 1440, "forecast_date": 30, 'lead': 1, 'quantiles': 1},
           auto_rechunk=False)
def salient_blend(start_time, end_time, variable, timescale="sub-seasonal", grid="global0_25"):  # noqa: ARG001
    """Processed Salient forecast files."""
    ds = salient_blend_raw(variable, timescale=timescale)
    ds = ds.dropna('forecast_date', how='all')

    # Regrid the data
    ds = regrid(ds, grid, base='base180', method='conservative')
    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['lead', 'prob_type', 'prob_threshold', 'grid', 'mask',
                       'region', 'groupby', 'use_ltn', 'first_year', 'last_year'],
           cache=False,
           timeseries='time')
def salient_spw(start_time, end_time, lead,
                prob_type='deterministic', prob_threshold=0.6,
                grid="global1_5", mask='lsm', region="global",
                groupby=['ea_rainy_season', 'year'],
                use_ltn=False, first_year=2004, last_year=2015):  # noqa: ARG001
    """Approximate suitable planting window from Salient weekly forecasts."""
    if use_ltn:
        raise NotImplementedError('Long-term normalization not implemented for ECMWF SPW forecasts.')
    if prob_type != 'deterministic':
        raise NotImplementedError("Only deterministic forecasts supported for Salient SPW.")

    lead_params = {f"day{i+1}": i for i in range(20)}
    lead_offset_days = lead_params.get(lead, None)
    if lead_offset_days is None:
        raise NotImplementedError(f"Lead {lead} not implemented for Salient SPW forecasts.")

    daily_ds = salient_blend(start_time, end_time, 'precip', timescale='sub-seasonal', grid=grid)

    # Select median as deterministic forecast
    daily_ds = daily_ds.sel(quantiles=0.5)  # TODO: should update this to enable probabilistic handling

    # What week does our lead fall in?
    week = lead_offset_days // 7 + 1  # Convert offset days to week
    daily_ds = daily_ds.sel(lead=week)
    daily_ds['lead'] = np.timedelta64(lead_offset_days, 'D').astype('timedelta64[ns]')

    # Time shift - we want target date, instead of forecast date
    daily_ds = shift_forecast_date_to_target_date(daily_ds, 'forecast_date', lead)
    daily_ds = daily_ds.rename({'forecast_date': 'time'})

    datasets = [(agg_days*daily_ds)
                .rename({'precip': f'precip_{agg_days}d'})
                for agg_days in [8, 11]]
    # Merge both datasets
    ds = xr.merge(datasets)

    # Apply masking
    ds = apply_mask(ds, mask, grid=grid)
    ds = clip_region(ds, region=region)

    rainy_onset_da = spw_rainy_onset(ds, groupby=groupby, time_dim='time',
                                     prob_dim='ensemble', prob_threshold=prob_threshold)
    rainy_onset_ds = rainy_onset_da.to_dataset(name='rainy_onset')
    return rainy_onset_ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask', 'region'])
def salient(start_time, end_time, variable, lead, prob_type='deterministic',
            grid='global0_25', mask='lsm', region='africa'):
    """Standard format forecast data for Salient."""
    lead_params = {
        "week1": ("sub-seasonal", 1),
        "week2": ("sub-seasonal", 2),
        "week3": ("sub-seasonal", 3),
        "week4": ("sub-seasonal", 4),
        "week5": ("sub-seasonal", 5),
        "month1": ("seasonal", 1),
        "month2": ("seasonal", 2),
        "month3": ("seasonal", 3),
        "quarter1": ("long-range", 1),
        "quarter2": ("long-range", 2),
        "quarter3": ("long-range", 3),
        "quarter4": ("long-range", 4),
    }
    if variable == 'rainy_onset':
        for i in range(1, 21):
            lead_params[f"day{i}"] = ("sub-seasonal", i)

    timescale, lead_id = lead_params.get(lead, (None, None))
    if timescale is None:
        raise NotImplementedError(f"Lead {lead} not implemented for Salient.")

    # Convert start and end time to forecast start and end based on lead time
    forecast_start = target_date_to_forecast_date(start_time, lead)
    forecast_end = target_date_to_forecast_date(end_time, lead)

    if variable == 'rainy_onset':
        ds = salient_spw(forecast_start, forecast_end, lead,
                         prob_type=prob_type, prob_threshold=0.6,
                         grid=grid, mask=mask, region=region,
                         groupby=['ea_rainy_season', 'year'])
    else:
        ds = salient_blend(forecast_start, forecast_end, variable, timescale=timescale, grid=grid)
        if prob_type == 'deterministic':
            # Get the median forecast
            ds = ds.sel(quantiles=0.5)
            # drop the quantiles dimension
            ds = ds.reset_coords("quantiles", drop=True)
            ds = ds.assign_attrs(prob_type="deterministic")
        elif prob_type == "probabilistic":
            # Set an attribute to say this is a quantile forecast
            ds = ds.rename({'quantiles': 'member'})
            ds = ds.assign_attrs(prob_type="quantile")
        else:
            raise ValueError("Invalid probabilistic type")

        # Get specific lead
        ds = ds.sel(lead=lead_id)

        # Time shift - we want target date, instead of forecast date
        ds = shift_forecast_date_to_target_date(ds, 'forecast_date', lead)
        ds = ds.rename({'forecast_date': 'time'})

        # Apply masking
        ds = apply_mask(ds, mask, var=variable, grid=grid)
        # Clip to specified region
        ds = clip_region(ds, region=region)

    return ds
