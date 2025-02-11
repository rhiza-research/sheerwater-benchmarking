"""Functions to fetch and process data from the ECMWF WeatherBench dataset."""
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
import xarray as xr
from functools import partial
from sheerwater_benchmarking.reanalysis import era5_rolled
from sheerwater_benchmarking.tasks import spw_rainy_onset, spw_precip_preprocess
from sheerwater_benchmarking.utils import (dask_remote, cacheable,
                                           apply_mask, clip_region,
                                           lon_base_change,
                                           roll_and_agg,
                                           lead_to_agg_days,
                                           regrid, get_variable,
                                           target_date_to_forecast_date,
                                           shift_forecast_date_to_target_date)


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'forecast_type', 'run_type', 'time_group', 'grid'],
           cache=False,
           timeseries=['start_date', 'model_issuance_date'],
           chunking={"lat": 121, "lon": 240, "lead_time": 46,
                     "start_date": 29, "start_year": 29,
                     "model_issuance_date": 1})
def ifs_extended_range_raw(start_time, end_time, variable, forecast_type,  # noqa ARG001
                           run_type='average', time_group='weekly', grid="global1_5"):
    """Fetches IFS extended range forecast data from the WeatherBench2 dataset.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        forecast_type (str): The type of forecast to fetch. One of "forecast" or "reforecast".
        run_type (str): The type of run to fetch. One of:
            - average: to download the averaged of the perturbed runs
            - perturbed: to download all perturbed runs
            - [int 0-50]: to download a specific  perturbed run
        time_group (str): The time grouping to use. One of: "daily", "weekly", "biweekly"
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
    """
    if grid != 'global1_5':
        raise NotImplementedError("Only global 1.5 degree grid is implemented.")

    forecast_str = "-reforecast" if forecast_type == "reforecast" else ""
    run_str = "_ens_mean" if run_type == "average" else ""
    avg_str = "-avg" if time_group == "daily" else "_avg"  # different naming for daily
    time_str = "" if time_group == "daily" else "-weekly" if time_group == "weekly" else "-biweekly"
    file_str = f'ifs-ext{forecast_str}-full-single-level{time_str}{avg_str}{run_str}.zarr'
    filepath = f'gs://weatherbench2/datasets/ifs_extended_range/{time_group}/{file_str}'

    # Pull the google dataset
    ds = xr.open_zarr(filepath, decode_timedelta=True)

    # Select the right variable
    var = get_variable(variable, 'ecmwf_ifs_er')
    ds = ds[var].to_dataset()
    ds = ds.rename_vars(name_dict={var: variable})

    # Convert local dataset naming and units
    ds = ds.rename({'latitude': 'lat', 'longitude': 'lon', 'prediction_timedelta': 'lead_time'})
    if run_type != 'average':
        ds = ds.rename({'number': 'member'})
    if forecast_type == 'reforecast':
        ds = ds.rename({'hindcast_year': 'start_year'})
        ds = ds.rename({'forecast_time': 'model_issuance_date'})
        ds = ds.drop('time')
    else:
        ds = ds.rename({'time': 'start_date'})

    ds = ds.drop('valid_time')

    # If a specific run, select
    if isinstance(run_type, int):
        ds = ds.sel(member=run_type)
    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'forecast_type', 'run_type', 'time_group', 'grid'],
           cache=True,
           timeseries=['start_date', 'model_issuance_date'],
           cache_disable_if={'grid': 'global1_5'},
           chunking={"lat": 121, "lon": 240, "lead_time": 1,
                     "start_date": 1000,
                     "model_issuance_date": 1000, "start_year": 1,
                     "member": 1},
           chunk_by_arg={
               'grid': {
                   # A note: a setting where time is in groups of 200 works better for regridding tasks,
                   # but is less good for storage.
                   'global0_25': {"lat": 721, "lon": 1440, 'model_issuance_date': 30, "start_date": 30}
               },
           },
           auto_rechunk=False)
def ifs_extended_range(start_time, end_time, variable, forecast_type,
                       run_type='average', time_group='weekly', grid="global1_5"):
    """Fetches IFS extended range forecast and reforecast data from the WeatherBench2 dataset.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        forecast_type (str): The type of forecast to fetch. One of "forecast" or "reforecast".
        run_type (str): The type of run to fetch. One of:
            - average: to download the averaged of the perturbed runs
            - perturbed: to download all perturbed runs
            - [int 0-50]: to download a specific  perturbed run
        time_group (str): The time grouping to use. One of: "daily", "weekly", "biweekly"
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
    """
    """IRI ECMWF average forecast with regridding."""
    ds = ifs_extended_range_raw(start_time, end_time, variable, forecast_type,
                                run_type, time_group=time_group, grid='global1_5')
    # Convert to base180 longitude
    ds = lon_base_change(ds, to_base="base180")

    if variable in ['tmp2m', 'tmax2m', 'tmin2m']:
        ds[variable] = ds[variable] - 273.15
        ds.attrs.update(units='C')
    elif variable == 'precip':
        ds[variable] = ds[variable] * 1000.0
        ds.attrs.update(units='mm')
        ds = np.maximum(ds, 0)
    elif variable == 'ssrd':
        ds.attrs.update(units='Joules/m^2')
        ds = np.maximum(ds, 0)
    if grid == 'global1_5':
        return ds
    # Regrid onto appropriate grid
    if forecast_type == 'reforecast':
        raise NotImplementedError("Regridding reforecast data should be done with extreme care. It's big.")

    # Need all lats / lons in a single chunk to be reasonable
    chunks = {'lat': 121, 'lon': 240, 'lead_time': 1}
    if run_type == 'perturbed':
        chunks['member'] = 1
        chunks['start_date'] = 200
    else:
        chunks['start_date'] = 200
    ds = ds.chunk(chunks)
    # Need all lats / lons in a single chunk for the output to be reasonable
    ds = regrid(ds, grid, base='base180', method='conservative',
                output_chunks={"lat": 721, "lon": 1440})
    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'lead', 'run_type', 'time_group', 'grid'],
           timeseries=['model_issuance_date'],
           cache=True,
           chunking={"lat": 121, "lon": 240, "lead_time": 1, "model_issuance_date": 200, "member": 50})
def ifs_er_reforecast_lead_bias(start_time, end_time, variable, lead=0, run_type='average',
                                time_group='weekly', grid="global1_5"):
    """Computes the bias of ECMWF reforecasts for a specific lead."""
    # Fetch the reforecast data; get's the past 20 years associated with each start date
    ds_deb = ifs_extended_range(start_time, end_time, variable, forecast_type="reforecast",
                                run_type=run_type, time_group=time_group, grid=grid)

    # Get the appropriate lead
    n_leads = len(ds_deb.lead_time)
    if lead >= n_leads:
        # Lead does not exist
        return None
    ds_deb = ds_deb.sel(lead_time=np.timedelta64(lead, 'D'))

    first_date = ds_deb.model_issuance_date.min().values
    last_date = ds_deb.model_issuance_date.max().values
    # We need ERA5 data from the start_time to 20 years before the first date
    new_start = (pd.Timestamp(first_date) - relativedelta(years=20)).strftime("%Y-%m-%d")
    # We need ERA5 data from the end time to the end time plus last lead in days
    new_end = (pd.Timestamp(last_date + ds_deb.lead_time.values) - relativedelta(years=1)).strftime("%Y-%m-%d")

    # Get the pre-aggregated ERA5 data
    agg = {'daily': 1, 'weekly': 7, 'biweekly': 14}[time_group]
    ds_truth = era5_rolled(new_start, new_end, variable, agg_days=agg, grid=grid)

    def get_bias(ds_sub):
        """Get the 20-year estimated bias of the reforecast data."""
        # The the corresponding forecast dates for the reforecast data
        dates = [np.datetime64(pd.Timestamp(ds_sub['model_issuance_date'].values[0] + relativedelta(years=x)))
                 for x in ds_sub.start_year]

        # Adjust each forecast date by the lead time (0, 1, 2, ... days)
        lead_td = ds_deb.lead_time.values
        lead_dates = [x + lead_td for x in dates]

        # Select the subset of the ground truth matching this lead
        ds_truth_lead = ds_truth.sel(time=lead_dates)

        # # Assign the time coordinate to match the forecast dataframe for alignment and subtract
        ds_truth_lead = ds_truth_lead.assign_coords(time=ds_sub.start_year.values).rename(time='start_year')
        bias = (ds_truth_lead - ds_sub).mean(dim='start_year')
        return bias

    bias = ds_deb.groupby('model_issuance_date').map(get_bias)
    return bias


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'run_type', 'time_group', 'grid'],
           timeseries=['model_issuance_date'],
           cache=True,
           chunking={"lat": 121, "lon": 240, "lead_time": 1, "model_issuance_date": 1000, "member": 1})
def ifs_er_reforecast_bias(start_time, end_time, variable, run_type='average', time_group='weekly', grid="global1_5"):
    """Computes the bias of ECMWF reforecasts for all leads."""
    # Fetch the reforecast data to calculate how many leads we need
    if time_group == 'weekly':
        leads = [0, 7, 14, 21, 28, 35]
    elif time_group == 'biweekly':
        leads = [0, 7, 14, 21, 28]
    elif time_group == 'daily':
        leads = list(range(46))
    else:
        raise NotImplementedError(f"Time group {time_group} not implemented for ECMWF reforecasts.")

    # Accumulate all the per lead biases
    biases = []
    for i in leads:
        biases.append(ifs_er_reforecast_lead_bias(start_time, end_time, variable, lead=i,
                                                  run_type=run_type, time_group=time_group, grid=grid))
    # Concatenate leads and unstack
    ds_biases = xr.concat(biases, dim='lead_time')
    return ds_biases


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'margin_in_days', 'run_type', 'time_group', 'grid'],
           cache=True,
           timeseries=['start_date'],
           chunking={"lat": 121, "lon": 240, "lead_time": 1,
                     "start_date": 1000,
                     "model_issuance_date": 1000, "start_year": 1,
                     "member": 1},
           chunk_by_arg={
               'grid': {
                   # A note: a setting where time is in groups of 200 works better for regridding tasks,
                   # but is less good for storage.
                   'global0_25': {"lat": 721, "lon": 1440, 'model_issuance_date': 30, "start_date": 30}
               },
           })
def ifs_extended_range_debiased(start_time, end_time, variable, margin_in_days=6,
                                run_type='average', time_group='weekly', grid="global1_5"):
    """Computes the debiased ECMWF forecasts."""
    # Get bias data from reforecast; for now, debias with deterministic bias
    ds_b = ifs_er_reforecast_bias(start_time, end_time, variable,
                                  run_type='average', time_group=time_group, grid=grid)

    # Get forecast data
    ds_f = ifs_extended_range(start_time, end_time, variable, forecast_type='forecast',
                              run_type=run_type, time_group=time_group, grid=grid)
    if time_group == 'weekly':
        leads = [np.timedelta64(x, 'D') for x in [0, 7, 14, 21, 28, 35]]
    elif time_group == 'biweekly':
        leads = [np.timedelta64(x, 'D') for x in [0, 7, 14, 21, 28]]
    elif time_group == 'daily':
        leads = [np.timedelta64(x, 'D') for x in range(46)]
    else:
        raise NotImplementedError(f"Time group {time_group} not implemented for ECMWF reforecasts.")
    ds_f = ds_f.sel(lead_time=leads)

    def bias_correct(ds_sub, mid=6):
        date = ds_sub.start_date.values
        dt = np.timedelta64(mid, 'D')
        nbhd = (ds_b.model_issuance_date.values >= date - dt) & \
            (ds_b.model_issuance_date.values <= date + dt)
        if nbhd.sum() == 0:  # No data to debias
            raise ValueError('No debiasing data found.')

        nbhd_df = ds_b.isel(model_issuance_date=nbhd).mean(dim='model_issuance_date')
        dsp = ds_sub + nbhd_df
        return dsp

    ds = ds_f.groupby('start_date').map(bias_correct, mid=margin_in_days)
    # Should not be below zero after bias correction
    if variable in ['precip', 'ssrd']:
        ds = np.maximum(ds, 0)
    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'margin_in_days', 'run_type', 'time_group', 'grid'],
           cache=True,
           timeseries=['start_date'],
           cache_disable_if={'grid': 'global1_5'},
           chunking={"lat": 121, "lon": 240, "lead_time": 1,
                     "start_date": 1000,
                     "model_issuance_date": 1000, "start_year": 1,
                     "member": 1},
           chunk_by_arg={
               'grid': {
                   # A note: a setting where time is in groups of 200 works better for regridding tasks,
                   # but is less good for storage.
                   'global0_25': {"lat": 721, "lon": 1440, 'model_issuance_date': 30, "start_date": 30}
               },
           })
def ifs_extended_range_debiased_regrid(start_time, end_time, variable, margin_in_days=6,
                                       run_type='average', time_group='weekly', grid="global1_5"):
    """Computes the debiased ECMWF forecasts."""
    ds = ifs_extended_range_debiased(start_time, end_time, variable, margin_in_days=margin_in_days,
                                     run_type=run_type, time_group=time_group, grid='global1_5')
    if grid == 'global1_5':
        return ds

    # Need all lats / lons in a single chunk to be reasonable
    chunks = {'lat': 121, 'lon': 240, 'lead_time': 1}
    if run_type == 'perturbed':
        chunks['member'] = 1
        chunks['start_date'] = 200
    else:
        chunks['start_date'] = 200
    ds = ds.chunk(chunks)
    # Need all lats / lons in a single chunk for the output to be reasonable
    ds = regrid(ds, grid, base='base180', method='conservative',
                output_chunks={"lat": 721, "lon": 1440})
    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'prob_type', 'agg_days', 'grid', 'debiased'],
           cache=True,
           timeseries=['start_date'],
           cache_disable_if={'agg_days': [1, 7, 14]},
           chunking={"lat": 121, "lon": 240, "lead_time": 1,
                     "start_date": 1000, "member": 1},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, "start_date": 30}
               },
           })
def ifs_extended_range_rolled(start_time, end_time, variable,
                              prob_type='deterministic', agg_days=7,
                              grid="global1_5", debiased=True):
    """Standard format forecast data for aggregated ECMWF forecasts."""
    run_type = 'perturbed' if prob_type == 'probabilistic' else 'average'
    if debiased:
        fn = ifs_extended_range_debiased_regrid
        kwargs = {'margin_in_days': 6}
    else:
        fn = ifs_extended_range
        kwargs = {'forecast_type': 'forecast'}

    if agg_days not in [1, 7, 14]:  # not one of the precomputed time groups
        ds = fn(start_time, end_time, variable,
                run_type=run_type, time_group='daily', grid=grid, **kwargs)
        # Get aggregated variable
        ds[variable] = roll_and_agg(ds[variable], agg=agg_days, agg_col='lead_time', agg_fn='mean')
    else:
        time_group = {1: 'daily', 7: 'weekly', 14: 'biweekly'}[agg_days]
        ds = fn(start_time, end_time, variable,
                run_type=run_type, time_group=time_group, grid=grid, **kwargs)
    return ds


def _process_lead(variable, lead):
    """Helper function for interpreting lead for ECMWF forecasts."""
    lead_params = {}
    if variable == 'rainy_onset':  # rainy onset only has daily leads out to day 36
        lead_params = {f"day{i+1}": i for i in range(36)}
    elif variable == 'rainy_onset_no_drought':
        # need to add 11 days to the lead to handle drought condition
        lead_params = {f"day{i+1}": i for i in range(25)}
    else:
        for i in range(46):
            lead_params[f"day{i+1}"] = i
        for i in [0, 7, 14, 21, 28, 35]:
            lead_params[f"week{i//7+1}"] = i
        for i in [0, 7, 14, 21, 28]:
            lead_params[f"weeks{(i//7)+1}{(i//7)+2}"] = i
    lead_offset_days = lead_params.get(lead, None)
    if lead_offset_days is None:
        raise NotImplementedError(f"Lead {lead} not implemented for ECMWF {variable} forecasts.")

    agg_days = lead_to_agg_days(lead)
    return agg_days, lead_offset_days


@dask_remote
def ecmwf_ifs_spw(start_time, end_time, lead, debiased=True,
                  prob_type='probabilistic', prob_threshold=0.6,
                  onset_group=['ea_rainy_season', 'year'], aggregate_group=None,
                  drought_condition=False,
                  grid='global1_5', mask='lsm', region="global"):
    """The ECMWF SPW forecasts."""
    # Get rainy season onset forecast
    prob_label = prob_type if prob_type == 'deterministic' else 'ensemble'

    # Set up aggregation and shift functions for SPW
    agg_fn = partial(ifs_extended_range_rolled, start_time, end_time, variable='precip',
                     prob_type=prob_type, grid=grid, debiased=debiased)

    def shift_fn(ds, shift_by_days):
        """Helper function for selecting and shifting lead for ECMWF forecasts."""
        # Select the appropriate lead
        lead_offset_days = _process_lead('precip', lead)[1]
        lead_sel = {'lead_time': np.timedelta64(lead_offset_days + shift_by_days, 'D')}
        ds = ds.sel(**lead_sel)
        # Time shift - we want target date, instead of forecast date
        ds = shift_forecast_date_to_target_date(ds, 'start_date', lead)
        ds = ds.rename({'start_date': 'time'})
        return ds

    roll_days = [8, 11] if not drought_condition else [8, 11, 11]
    shift_days = [0, 0] if not drought_condition else [0, 0, 11]
    data = spw_precip_preprocess(agg_fn, shift_fn, agg_days=roll_days, shift_days=shift_days,
                                 mask=mask, region=region, grid=grid)

    (prob_dim, prob_threshold) = ('member', prob_threshold) if prob_type == 'probabilistic' else (None, None)
    ds = spw_rainy_onset(data,
                         onset_group=onset_group, aggregate_group=aggregate_group,
                         time_dim='time',
                         prob_type=prob_label, prob_dim=prob_dim, prob_threshold=prob_threshold,
                         drought_condition=drought_condition,
                         mask=mask, region=region, grid=grid)
    return ds


@dask_remote
def _ecmwf_ifs_er_unified(start_time, end_time, variable, lead, prob_type='deterministic',
                          grid="global1_5", mask='lsm', region="global", debiased=True):
    """Unified API accessor for ECMWF raw and debiased forecasts."""
    agg_days, lead_offset_days = _process_lead(variable, lead)

    forecast_start = target_date_to_forecast_date(start_time, lead)
    forecast_end = target_date_to_forecast_date(end_time, lead)

    prob_label = prob_type if prob_type == 'deterministic' else 'ensemble'
    if variable == 'rainy_onset' or variable == 'rainy_onset_no_drought':
        drought_condition = variable == 'rainy_onset_no_drought'
        ds = ecmwf_ifs_spw(forecast_start, forecast_end, lead, debiased=debiased,
                           prob_type=prob_type, prob_threshold=0.6,
                           onset_group=['ea_rainy_season', 'year'], aggregate_group=None,
                           drought_condition=drought_condition,
                           grid=grid, mask=mask, region=region)
        # Rainy onset is sparse, so we need to set the sparse attribute
        ds = ds.assign_attrs(sparse=True)
    else:
        ds = ifs_extended_range_rolled(forecast_start, forecast_end, variable, prob_type=prob_type,
                                       agg_days=agg_days, grid=grid, debiased=debiased)
        # Select the appropriate lead
        lead_sel = {'lead_time': np.timedelta64(lead_offset_days, 'D')}
        ds = ds.sel(**lead_sel)
        ds = shift_forecast_date_to_target_date(ds, 'start_date', lead)
        ds = ds.rename({'start_date': 'time'})

        # Apply masking and clip to region
        ds = apply_mask(ds, mask, grid=grid)
        ds = clip_region(ds, region=region)

    # Assign probability label
    ds = ds.assign_attrs(prob_type=prob_label)
    if 'spatial_ref' in ds.variables:
        ds = ds.drop_vars('spatial_ref')

    # TODO: remove this once we update ECMWF caches
    if variable == 'precip' and agg_days in [7, 14]:
        print("Warning: Dividing precip by days to get daily values. Do you still want to do this?")
        ds['precip'] /= agg_days

    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask', 'region'])
def ecmwf_ifs_er(start_time, end_time, variable, lead, prob_type='deterministic',
                 grid='global1_5', mask='lsm', region="global"):
    """Standard format forecast data for ECMWF forecasts."""
    return _ecmwf_ifs_er_unified(start_time, end_time, variable, lead, prob_type=prob_type,
                                 grid=grid, mask=mask, region=region, debiased=False)


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask', 'region'])
def ecmwf_ifs_er_debiased(start_time, end_time, variable, lead, prob_type='deterministic',
                          grid='global1_5', mask='lsm', region="global"):
    """Standard format forecast data for ECMWF forecasts."""
    return _ecmwf_ifs_er_unified(start_time, end_time, variable, lead, prob_type=prob_type,
                                 grid=grid, mask=mask, region=region, debiased=True)
