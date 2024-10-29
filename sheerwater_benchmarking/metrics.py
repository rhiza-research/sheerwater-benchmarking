"""Verification metrics for forecasts."""
from importlib import import_module

import pandas as pd
import xarray as xr

from sheerwater_benchmarking.utils import cacheable, dask_remote, clip_region, is_valid
from weatherbench2.metrics import _spatial_average


def get_datasource_fn(datasource):
    """Import the datasource function from any available source."""
    try:
        mod = import_module("sheerwater_benchmarking.reanalysis")
        fn = getattr(mod, datasource)
    except (ImportError, AttributeError):
        try:
            mod = import_module("sheerwater_benchmarking.forecasts")
            fn = getattr(mod, datasource)
        except (ImportError, AttributeError):
            try:
                mod = import_module("sheerwater_benchmarking.baselines")
                fn = getattr(mod, datasource)
            except (ImportError, AttributeError):
                raise ImportError(f"Could not find datasource {datasource}.")

    return fn


def get_metric_fn(prob_type, metric, spatial=True):
    """Import the correct metrics function from weatherbench."""
    # Make sure things are consistent
    if prob_type == 'deterministic' and metric == 'crps':
        raise ValueError("Cannot run CRPS on deterministic forecasts.")
    elif (prob_type == 'ensemble' or prob_type == 'quantile') and metric == 'mae':
        raise ValueError("Cannot run MAE on probabilistic forecasts.")

    wb_metrics = {
        'crps': ('xskillscore', 'crps_ensemble', {}),
        'crps-q': ('weatherbench2.metrics', 'QuantileCRPS', {'quantile_dim': 'member'}),
        'spatial-crps': ('xskillscore', 'crps_ensemble', {'dim': 'time'}),
        'spatial-crps-q': ('weatherbench2.metrics', 'SpatialQuantileCRPS', {'quantile_dim': 'member'}),
        'mae': ('weatherbench2.metrics', 'MAE', {}),
        'spatial-mae': ('weatherbench2.metrics', 'SpatialMAE', {}),
    }

    if spatial:
        metric = 'spatial-' + metric

    if prob_type == 'quantile':
        metric = metric + '-q'

    try:
        metric_lib, metric_mod, metric_kwargs = wb_metrics[metric]
        mod = import_module(metric_lib)
        metric_fn = getattr(mod, metric_mod)
        return metric_fn, metric_kwargs, metric_lib
    except (ImportError, AttributeError):
        raise ImportError(f"Did not find implementation for metric {metric}")


@dask_remote
@cacheable(data_type='array',
           timeseries=['time'],
           cache_args=['variable', 'lead', 'forecast',
                       'truth', 'metric', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'time': 30}
               },
           },
           cache=True)
def global_metric(start_time, end_time, variable, lead, forecast, truth,
                  metric, grid="global1_5", mask='lsm', region='global'):
    """Compute a metric without aggregated in time or space at a specific lead."""
    if metric == "crps":
        prob_type = "probabilistic"
    elif metric == "mae":
        prob_type = "deterministic"
    else:
        raise ValueError("Unsupported metric")

    # Get the forecast
    fcst_fn = get_datasource_fn(forecast)
    fcst = fcst_fn(start_time, end_time, variable, lead=lead,
                   prob_type=prob_type, grid=grid, mask=mask, region=region)

    # Get the truth to compare against
    truth_fn = get_datasource_fn(truth)
    obs = truth_fn(start_time, end_time, variable, lead=lead, grid=grid, mask=mask, region=region)

    # Check to see the prob type attribute
    enhanced_prob_type = fcst.attrs['prob_type']

    metric_fn, metric_kwargs, metric_lib = get_metric_fn(enhanced_prob_type, metric, spatial=True)

    # Run the metric without aggregating in time or space
    if metric_lib == 'xskillscore':
        assert prob_type == 'probabilistic'
        fcst = fcst.chunk(member=-1, time=1, lat=100, lon=100)
        m_ds = metric_fn(observations=obs, forecasts=fcst, mean=False, **metric_kwargs)
    else:
        m_ds = metric_fn(**metric_kwargs).compute(forecast=fcst, truth=obs, avg_time=False, skipna=True)
        m_ds = m_ds.rename({'latitude': 'lat', 'longitude': 'lon'})

    return m_ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['start_time', 'end_time', 'variable', 'lead', 'forecast',
                       'truth', 'metric', 'time_grouping', 'spatial', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": -1},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, "time": 30}
               },
           },
           cache=True)
def grouped_metric(start_time, end_time, variable, lead, forecast, truth,
                   metric, time_grouping=None, spatial=False, grid="global1_5",
                   mask='lsm', region='africa'):
    """Compute a grouped metric for a forecast at a specific lead."""
    # Get the unaggregated metric
    ds = global_metric(start_time, end_time, variable, lead, forecast, truth,
                       metric, grid, mask, region='global')

    # Check to make sure it supports this region/time
    if not is_valid(ds, variable, mask, region, grid, valid_threshold=0.99):
        raise NotImplementedError("Forecast not implemented for these parameters.")

    # Clip it to the region
    ds = clip_region(ds, region)

    # Group the time column based on time grouping
    if time_grouping:
        if time_grouping == 'month_of_year':
            # TODO if you want this as a name: ds.coords["time"] = ds.time.dt.strftime("%B")
            ds.coords["time"] = ds.time.dt.month
        elif time_grouping == 'year':
            ds.coords["time"] = ds.time.dt.year
        elif time_grouping == 'quarter_of_year':
            ds.coords["time"] = ds.time.dt.quarter
        else:
            raise ValueError("Invalid time grouping")

        ds = ds.groupby("time").mean()
    else:
        # Average in time
        ds = ds.mean(dim="time")

    for coord in ds.coords:
        if coord not in ['time', 'lat', 'lon']:
            ds = ds.reset_coords(coord, drop=True)

    # Average in space
    if spatial:
        return ds
    else:
        return _spatial_average(ds, lat_dim='lat', lon_dim='lon', skipna=True)


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'lead', 'forecast', 'truth', 'metric', 'baseline',
                       'time_grouping', 'spatial', 'grid', 'mask', 'region'],
           cache=False)
def skill_metric(start_time, end_time, variable, lead, forecast, truth,
                 metric, baseline, time_grouping=None, spatial=False, grid="global1_5",
                 mask='lsm', region='global'):
    """Compute skill either spatially or as a region summary."""
    m_ds = grouped_metric(start_time, end_time, variable, lead, forecast,
                          truth, metric, time_grouping, spatial=spatial, grid=grid, mask=mask, region=region)

    # Get the baseline if it exists and run its metric
    base_ds = grouped_metric(start_time, end_time, variable, lead, baseline,
                             truth, metric, time_grouping, spatial=spatial, grid=grid, mask=mask, region=region)

    print("Got metrics. Computing skill")

    # Compute the skill
    # TODO - think about base metric of 0
    m_ds = (1 - (m_ds/base_ds))

    return m_ds


@dask_remote
@cacheable(data_type='tabular',
           cache_args=['start_time', 'end_time', 'variable', 'truth', 'metric', 'baseline',
                       'time_grouping', 'grid', 'mask', 'region'],
           cache=True)
def summary_metrics_table(start_time, end_time, variable,
                          truth, metric, time_grouping=None, baseline=None,
                          grid='global1_5', mask='lsm', region='global'):
    """Runs summary metric repeatedly for all forecasts and creates a pandas table out of them."""
    forecasts = ['salient', 'ecmwf_ifs_er', 'ecmwf_ifs_er_debiased', 'climatology_2015']
    leads = ["week1", "week2", "week3", "week4", "week5"]

    # Turn the dict into a pandas dataframe with appropriate columns
    leads_skill = [lead + '_skill' for lead in leads]

    # Create a list to insert our data
    results = []

    for forecast in forecasts:
        forecast_ds = None
        forecast_dict = {'forecast': forecast}
        for i, lead in enumerate(leads):

            print(f"""Running for {forecast} and {lead} with variable {variable},
                      metric {metric}, grid {grid}, and region {region}""")
            # First get the value without the baseline
            ds = grouped_metric(start_time, end_time, variable, lead, forecast, truth,
                                metric, time_grouping, False, grid, mask, region)

            # If there is a time grouping keep as xarray for combining
            # otherwise keep scalar value in dict
            if time_grouping:
                if forecast_ds:
                    ds = ds.rename({variable: lead})
                    forecast_ds = xr.combine_by_coords([forecast_ds, ds])
                else:
                    forecast_ds = ds.rename({variable: lead})
            else:
                forecast_dict[lead] = float(ds[variable].values)

            # IF there is a baseline get the skill
            if baseline:
                skill_ds = skill_metric(start_time, end_time, variable, lead, forecast, truth,
                                        metric, baseline, time_grouping, False, grid, mask, region)

                if time_grouping:
                    # If there is a time grouping merge the two dataframes
                    # Rename the variable
                    skill_ds = skill_ds.rename({variable: leads_skill[i]})

                    forecast_ds = xr.combine_by_coords([forecast_ds, skill_ds])
                else:
                    # Otherwise just append it to the row
                    forecast_dict[leads_skill[i]] = float(skill_ds[variable].values)

        if time_grouping:
            # prep the rows from the dataset
            df = forecast_ds.to_pandas()

            # Add a column for the forecast
            df['forecast'] = forecast
            df = df.reset_index().rename(columns={'index': 'time'})

            # append all the rows to the results
            results = results + df.to_dict(orient='records')
        else:
            results.append(forecast_dict)

    # create the dataframe
    df = pd.DataFrame.from_records(results, index='forecast')

    # Rename the index
    df = df.reset_index().rename(columns={'index': 'forecast'})

    print(df)
    return df
