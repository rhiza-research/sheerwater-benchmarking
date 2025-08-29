"""Pulls Salient Predictions S2S forecasts from the Salient API."""
import xarray as xr
import numpy as np

from sheerwater_benchmarking.utils import (cacheable, dask_remote, get_variable, apply_mask, clip_region, regrid,
                                           forecast, convert_lead_to_valid_time, convert_to_standard_lead,
                                           get_forecast_start_end)


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


# @dask_remote
# @cacheable(data_type='array',
#            cache_args=['lead',
#                        'prob_type', 'prob_dim', 'prob_threshold',
#                        'onset_group', 'aggregate_group',
#                        'grid', 'mask', 'region'],
#            cache=False,
#            timeseries='time')
# def salient_spw(start_time, end_time, lead,
#                 prob_type='deterministic',
#                 onset_group=['ea_rainy_season', 'year'], aggregate_group=None,
#                 grid="global1_5", mask='lsm', region="global"):
#     """Approximate suitable planting window from Salient weekly forecasts."""
#     if prob_type != 'deterministic':
#         raise NotImplementedError("Only deterministic forecasts supported for Salient SPW.")

#     lead_params = {f"day{i+1}": i for i in range(25)}
#     lead_offset_days = lead_params.get(lead, None)
#     if lead_offset_days is None:
#         raise NotImplementedError(f"Lead {lead} not implemented for Salient SPW forecasts.")

#     daily_ds = salient_blend(start_time, end_time, 'precip', timescale='sub-seasonal', grid=grid)

#     # Select median as deterministic forecast
#     daily_ds = daily_ds.sel(quantiles=0.5)  # TODO: should update this to enable probabilistic handling

#     # What week does our lead fall in?
#     week = lead_offset_days // 7 + 1  # Convert offset days to week
#     daily_ds = daily_ds.sel(lead=week)
#     daily_ds['lead'] = np.timedelta64(lead_offset_days, 'D').astype('timedelta64[ns]')

#     # Time shift - we want target date, instead of forecast date
#     daily_ds = shift_forecast_date_to_target_date(daily_ds, 'forecast_date', lead)
#     daily_ds = daily_ds.rename({'forecast_date': 'time'})

#     datasets = [(agg_days*daily_ds)
#                 .rename({'precip': f'precip_{agg_days}d'})
#                 for agg_days in [8, 11]]
#     # Merge both datasets
#     ds = xr.merge(datasets)

#     # Apply masking
#     ds = apply_mask(ds, mask, grid=grid)
#     ds = clip_region(ds, region=region)

#     ds = spw_rainy_onset(ds, onset_group=onset_group, aggregate_group=aggregate_group,
#                                      time_dim='time', prob_type='deterministic')
#     return ds


@forecast
@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask', 'region'])
def salient(start_time, end_time, variable, lead, prob_type='deterministic',
            grid='global0_25', mask='lsm', region='africa'):
    """Standard format forecast data for Salient."""
    if variable == 'rainy_onset' or variable == 'rainy_onset_no_drought':
        raise NotImplementedError("Rainy onset forecasts not implemented for Salient.")

    lead_params = {
        "weekly": "sub-seasonal",
        "monthly": "seasonal",
        "quarterly": "long-range",
    }
    timescale = lead_params.get(lead, None)
    if timescale is None:
        raise NotImplementedError(f"Lead {lead} not implemented for Salient.")

    # Get the data with the right days
    forecast_start, forecast_end = get_forecast_start_end(lead, start_time, end_time)

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

    # Convert salient lead naming to match our standard
    if timescale == "sub-seasonal":
        ds = ds.assign_coords(lead_time=('lead', [np.timedelta64(i-1, 'W') for i in ds.lead.values]))
    elif timescale == "long-range":
        ds = ds.assign_coords(lead_time=('lead', [np.timedelta64((i-1)*120, 'D') for i in ds.lead.values]))
    elif timescale == "seasonal":
        # TODO: salient's monthly leads are 31 days, but we define them as 30 days
        ds = ds.assign_coords(lead_time=('lead', [i-np.timedelta64(1, 'D') for i in ds.lead.values]))
    else:
        raise ValueError(f"Invalid timescale: {timescale}")
    ds = ds.swap_dims({'lead': 'lead_time'})
    ds = ds.drop_vars('lead')

    # Create a new coordinate for valid_time, that is the start_date plus the lead time
    ds = convert_lead_to_valid_time(ds, initialization_date_dim='forecast_date')

    # Convert to standard lead
    ds = convert_to_standard_lead(ds, lead)

    # Time shift - we want target date, instead of forecast date
    ds = ds.sortby(ds.time)

    # Apply masking
    ds = apply_mask(ds, mask, var=variable, grid=grid)
    # Clip to specified region
    ds = clip_region(ds, region=region)

    return ds
