"""The Suitable Planting Window (SPW) tasks for the benchmarking platform."""
from functools import partial

from sheerwater_benchmarking.utils import groupby_time
from sheerwater_benchmarking.utils import first_satisfied_date, average_time, convert_to_datetime


def rainy_onset_condition(da, prob_dim='member', prob_threshold=0.6, time_dim='time'):
    """Condition for the rainy season onset.

    Requires the input data to have 11d and 8d precipitation aggregations.

    If probability dimension is not present, return a boolean array.

    If a probability dimension is present, and prob_threshold is None,
    return the probability of the condition being met.

    If a probability dimension is present, and prob_threshold is not None,
    return a boolean array based on the probability threshold.

    Args:
        da (xr.Dataset): Dataset to apply the condition to.
        prob_dim (str): Name of the ensemble dimension.
        prob_threshold (float): Threshold for the probability dimension.
        time_dim (str): Name of the time dimension.
    """
    # Ensure the data has the required aggregations
    if 'precip_11d' not in da.data_vars or 'precip_8d' not in da.data_vars:
        raise ValueError("Data must have 11d and 8d precipitation aggregations.")

    # Check aggregation against the thresholds in mm
    cond = (da['precip_11d'] > 40.) & (da['precip_8d'] > 30.0)
    if 'rainy_onset_ltn' in da.data_vars:
        cond &= (da[time_dim].dt.dayofyear >= da['rainy_onset_ltn'].dt.dayofyear)

    if prob_dim in da.dims:
        # If the probability dimension is present
        cond = cond.mean(dim=prob_dim)
        if prob_threshold is not None:
            # Convert to a boolean based on the probability threshold
            cond = cond > prob_threshold
    return cond


def spw_rainy_onset(ds, groupby, time_dim='time', prob_dim=None, prob_threshold=None):
    """Utility function to get first rainy onset."""
    # Ensure groupby is a list of lists
    if groupby and not isinstance(groupby, list):
        groupby = [groupby]
    if groupby and not isinstance(groupby[0], list):  # TODO: this is not fully general
        groupby = [groupby]

    # Compute the rainy season onset date for deterministic or thresholded probability forecasts
    if prob_threshold is None and prob_dim is not None:
        if groupby is not None:
            raise ValueError("Grouping is not supported for probabilistic forecasts without a threshold.")
        # Compute condition probability on the timeseries
        rainy_da = rainy_onset_condition(ds, prob_dim=prob_dim, prob_threshold=None, time_dim=time_dim)
        return rainy_da

    # Perform grouping
    agg_fn = [partial(first_satisfied_date,
                      condition=rainy_onset_condition,
                      time_dim=time_dim, base_time=None,
                      prob_dim=prob_dim, prob_threshold=prob_threshold)]

    # Returns a dataframe with a dimension 'time' corresponding to the first grouping value
    # and value 'rainy_onset' corresponding to the rainy season onset date
    if len(groupby) > 1:
        # For each additional grouping after the first, average over day of year within a grouping
        agg_fn += [partial(average_time, avg_over='time')]*(len(groupby)-1)
        # Add final conversion to datetime with no grouping
        agg_fn += [convert_to_datetime]
        groupby += [None]

    #  Apply the aggregation functions to the dataset using the groupby utility
    #  Set return_timeseries to True to get a dataset with a time dimension for each grouping
    rainy_da = groupby_time(ds, groupby=groupby, agg_fn=agg_fn,
                            time_dim=time_dim, return_timeseries=True)
    return rainy_da


# @dask_remote
# @cacheable(data_type='array',
#            cache_args=['truth', 'first_year', 'last_year', 'groupby', 'grid', 'mask', 'region'],
#            chunking={"lat": 121, "lon": 240, "time": 1000},
#            chunk_by_arg={
#                'grid': {
#                    'global0_25': {"lat": 721, "lon": 1440, "time": 30}
#                },
#            })
# def rainy_season_onset_ltn(truth='era5',
#                            first_year=2004, last_year=2015,
#                            groupby=[['quarter', 'year']],
#                            grid='global0_25', mask='lsm', region='global'):
#     """Get the rainy season onset from the long-term normals of a truth source (only ERA5 supported).

#     Args:
#         truth (str): Name of the truth source.
#         first_year (int): First year of the long-term normals.
#         last_year (int): Last year of the long-term normals.
#         groupby (list): List of grouping categories for the time dimension.
#             See `groupby_time` for more details on supported categories and format.
#             To get the rainy season onset:
#                 per year, pass `groupby=[['year']]`.
#                 per quarter per year, pass `groupby=[['quarter', 'year']]`.
#                 per ea_rainy_season per year, pass `groupby=[['ea_rainy_season', 'year']]`.
#                 per ea_rainy_season, averaged over years,
#                     pass `groupby=[['ea_rainy_season', 'year'], ['ea_rainy_season']]`.
#                 per year, averaged over years, pass `groupby=[['year'], [None]]]`.
#         grid (str): Name of the grid.
#         mask (str): Name of the mask.
#         region (str): Name of the region.
#     """
#     if truth != 'era5':
#         raise ValueError("Only ERA5 is supported for LTN constraints.")

#     ds = climatology_raw('precip', first_year=first_year, last_year=last_year, grid=grid)
#     # Mask and clip the region
#     ds = apply_mask(ds, mask, var='precip', grid=grid)
#     ds = clip_region(ds, region=region)

#     rainy_da = _get_first_rainy_onset(ds, groupby, time_dim='dayofyear')
#     rainy_ds = rainy_da.to_dataset(name='rainy_onset')
#     rainy_ds = rainy_ds.drop_vars(['spatial_ref', 'dayofyear'])
#     return rainy_ds


# @dask_remote
# def rainy_season_onset(ds,
#                        time_dim='time',
#                        use_ltn=False, first_year=2004, last_year=2015,
#                        groupby=[['quarter', 'year']]):
#     """Get for an input dataframe, with optional LTN constraint.

#     Args:
#         use_ltn (bool): Whether to use the long-term normals of ERA5 to constrain source.
#         first_year (int): First year of the long-term normals.
#         last_year (int): Last year of the long-term normals.
#         groupby (list): List of grouping categories for the time dimension.
#             See `groupby_time` for more details on supported categories and format.
#             To get the rainy season onset:
#                 per year, pass `groupby=[['year']]`.
#                 per quarter per year, pass `groupby=[['quarter', 'year']]`.
#                 per ea_rainy_season per year, pass `groupby=[['ea_rainy_season', 'year']]`.
#                 per ea_rainy_season, averaged over years,
#                     pass `groupby=[['ea_rainy_season', 'year'], ['ea_rainy_season']]`.
#                 per year, averaged over years, pass `groupby=[['year'], [None]]]`.
#     """
#     if use_ltn:
#         # Get the long-term normals for the first grouping category
#         if groupby[0][1] != 'year':
#             raise ValueError("Only year grouping is supported for LTN constraints.")

#         #  Get the corresponding long-term normal onset dates
#         ds_clim = rainy_season_onset_ltn(truth='era5', first_year=first_year, last_year=last_year,
#                                          groupby=groupby[0], grid=grid, mask=mask, region=region)
#         ds_clim = ds_clim.rename({'time': 'group_time'})
#         ds_clim = ds_clim.rename_vars({'rainy_onset': 'rainy_onset_ltn'})

#         # Assign the grouping coordinate of the first grouping category, assuming ['group', 'year'] format
#         ds = assign_grouping_coordinates(ds, groupby[0][0], time_dim='time')
#         ds = ds.assign_coords(
#             group_time=("time", convert_group_to_time(ds['group'], groupby[0][0])))

#         # Merge the long-term normals with the truth data
#         ds_clim = ds_clim.sel(group_time=ds['group_time'])
#         ds = ds.merge(ds_clim)

#     rainy_da = _get_first_rainy_onset(ds, groupby, time_dim='time')
#     rainy_ds = rainy_da.to_dataset(name='rainy_onset')
#     rainy_ds = rainy_ds.drop_vars('spatial_ref')
#     if 'dayofyear' in rainy_ds.coords:
#         rainy_ds = rainy_ds.drop_vars(['dayofyear', 'doy'])
#     rainy_ds = rainy_ds.chunk({'lat': 121, 'lon': 240, 'time': 1000})
#     return rainy_ds


# @dask_remote
# @cacheable(data_type='array',
#            cache_args=['forecast', 'prob_type', 'prob_threshold', 'use_ltn',
#                        'first_year', 'last_year', 'groupby', 'grid', 'mask', 'region'],
#            chunking={"lat": 121, "lon": 240, "time": 1000},
#            chunk_by_arg={
#                'grid': {
#                    'global0_25': {"lat": 721, "lon": 1440, "time": 30}
#                },
#            })
# def rainy_season_onset_forecast(start_time, end_time,
#                                 forecast,
#                                 prob_type='probabilistic',
#                                 prob_threshold=None,
#                                 use_ltn=False,
#                                 first_year=2004, last_year=2015,
#                                 groupby=[['quarter', 'year']],
#                                 grid='global1_5', mask='lsm', region='global'):
#     """Get the rainy reason onset from a given forecast.

#     Args:
#         start_time (str): Start date for the forecast data.
#         end_time (str): End date for the forecast data.
#         forecast (str): Name of the forecast source.
#         prob_type (str): Type of probabilistic forecast (either 'probabilistic' or 'deterministic').
#         prob_threshold (float): Threshold for the probability dimension.
#             If None, returns a probability of event occurrence per start date and lead.
#             If a threshold is passed, returns a specific date forecast.
#         use_ltn (bool): Whether to use the long-term normals of ERA5 to constrain forecast.
#         first_year (int): First year of the long-term normals.
#         last_year (int): Last year of the long-term normals.
#         groupby (list): List of grouping categories for the time dimension.
#             See `groupby_time` for more details on supported categories and format.
#         grid (str): Name of the grid.
#         mask (str): Name of the mask.
#         region (str): Name of the region.
#     """
#     # Get the forecast data in non-standard, daily, leads format from ECMWF
#     run_type = 'average' if prob_type == 'deterministic' else 'perturbed'
#     if forecast == "ecmwf_ifs_er":
#         ds = ifs_extended_range(start_time, end_time, 'precip', forecast_type='forecast',
#                                 run_type=run_type, time_group='daily', grid=grid)
#     elif forecast == "ecmwf_ifs_er_debiased":
#         ds = ifs_extended_range_debiased_regrid(start_time, end_time, 'precip',
#                                                 run_type=run_type, time_group='daily', grid=grid)
#     else:
#         raise ValueError("Only ECMWF IFS Extended Range and debiased forecasts are supported.")

#     # ECMWF doesn't support mask and region, so we need to apply them manually
#     # TODO: build out this interface more formally
#     ds = apply_mask(ds, mask, var='precip', grid=grid)
#     ds = clip_region(ds, region=region)

#     # Add a lead_date coordinate to the forecast
#     valid_time = ds['start_date'] + ds['lead_time']
#     ds = ds.assign_coords(valid_time=(['start_date', 'lead_time'], valid_time.data))

#     if use_ltn:
#         # Get the long-term normals for the first grouping category
#         if groupby[0][1] != 'year':
#             raise ValueError("Only year grouping is supported for LTN constraints.")

#         #  Get the corresponding long-term normal onset dates
#         ds_clim = rainy_season_onset_ltn(truth='era5', first_year=first_year, last_year=last_year,
#                                          groupby=groupby[0], grid=grid, mask=mask, region=region)
#         ds_clim = ds_clim.rename({'time': 'group_time'})
#         ds_clim = ds_clim.rename_vars({'rainy_onset': 'rainy_onset_ltn'})
#         # Create a new entry with NaT as the coordinate and a value of '1904-01-01'
#         pad = ds_clim.copy()
#         pad = pad.isel(group_time=0)
#         pad['group_time'] = pd.NaT
#         pad['rainy_onset_ltn'] = pd.to_datetime('1904-12-31')
#         ds_clim = xr.concat([ds_clim, pad], dim='group_time')

#         # Assign the grouping coordinate of the first grouping category, assuming ['group', 'year'] format
#         ds = assign_grouping_coordinates(ds, groupby[0][0], time_dim='valid_time')
#         ds = ds.assign_coords(
#             group_time=(["start_date", "lead_time"], convert_group_to_time(ds['group'], groupby[0][0])))

#         # Merge the long-term normals with the truth data
#         ds_clim = ds_clim.sel(group_time=ds['group_time'])
#         ds = ds.merge(ds_clim)

#     # Chain together the aggregation functions needed to compute the rainy season onset per grouping
#     if prob_type == 'deterministic':
#         agg_fn = partial(first_rainy_onset, time_dim='lead_time', time_offset='start_date')
#     else:
#         # If a non-NaN forecast is passed, produce a specific date forecast
#         agg_fn = partial(first_rainy_onset, time_dim='lead_time', time_offset='start_date',
#                          prob_dim='member', prob_threshold=prob_threshold)
#     # Apply the aggregation function over lead time along the start_date dimension
#     rainy_da = groupby_time(ds,
#                             groupby=None,
#                             agg_fn=agg_fn,
#                             time_dim='start_date',
#                             return_timeseries=True)

#     rainy_ds = rainy_da.to_dataset(name='rainy_forecast')
#     # TODO: why is chunking not working?
#     rainy_ds = rainy_ds.chunk(-1)
#     # TODO: Boolean grouping doesn't maintain the mask and region, so we need to apply them manually
#     rainy_ds = apply_mask(rainy_ds, mask, var='rainy_forecast', grid=grid)
#     rainy_ds = clip_region(rainy_ds, region=region)

#     # Assign grouping coordinates to forecast and merge with truth
#     rainy_ds = assign_grouping_coordinates(rainy_ds, groupby[0], time_dim='valid_time')
#     rainy_ds = rainy_ds.assign_coords(
#         time=("start_date", convert_group_to_time(rainy_ds['group'], groupby[0])))

#     return rainy_ds


# @dask_remote
# @cacheable(data_type='array',
#            cache_args=['truth', 'forecast', 'use_ltn', 'first_year', 'last_year',
#                        'groupby', 'prob_type', 'prob_threshold', 'grid', 'mask', 'region'],
#            chunking={"lat": 121, "lon": 240, "time": 1000},
#            chunk_by_arg={
#                'grid': {
#                    'global0_25': {"lat": 721, "lon": 1440, "time": 30}
#                },
#            },
#            cache=False)
# def rainy_season_onset_error(start_time, end_time,
#                              truth, forecast,
#                              use_ltn=False, first_year=2004, last_year=2015,
#                              groupby=[['quarter', 'year']],
#                              prob_type='probabilistic',
#                              prob_threshold=0.60,
#                              grid='global0_25', mask='lsm', region='global'):
#     """Compute the error between the rainy season onset from a given truth and forecast."""
#     truth_ds = rainy_season_onset_truth(
#         start_time, end_time, truth=truth,
#         use_ltn=use_ltn, first_year=first_year, last_year=last_year,
#         groupby=groupby, grid=grid, mask=mask, region=region)
#     forecast_ds = rainy_season_onset_forecast(
#         start_time, end_time, forecast, prob_type=prob_type, prob_threshold=prob_threshold,
#         grid=grid, mask=mask, region=region)

#     forecast_ds = forecast_ds.drop_vars('group')
#     truth_expanded = truth_ds.sel(time=forecast_ds['time'])
#     ds = xr.merge([truth_expanded, forecast_ds])

#     import pdb
#     pdb.set_trace()
#     if use_ltn:
#         # Truncate forecast probabilities to the LTN bounds
#         ltn_bounds = rainy_season_onset_truth(
#             start_time, end_time, truth='ltn',
#             use_ltn=False, first_year=first_year, last_year=last_year,
#             groupby=groupby, grid=grid, mask=mask, region=region)
#         ltn_bounds = ltn_bounds['rainy_onset'].values
#         forecast_ds = forecast_ds.where(forecast_ds['rainy_forecast'] > ltn_bounds[0], drop=True)
#         forecast_ds = forecast_ds.where(forecast_ds['rainy_forecast'] < ltn_bounds[1], drop=True)

#     # Compute derived metrics
#     # How does the model perform in terms of days of error per start date?
#     ds['error'] = (ds['rainy_forecast'] - ds['rainy_onset']).dt.days
#     # How does the model perform as we approach the rainy reason?
#     ds['look_ahead'] = (ds['start_date'] - ds['rainy_onset']).dt.days
#     # What lead (in days) was the forecast made at?
#     ds['lead'] = (ds['rainy_forecast'] - ds['start_date']).dt.days

#     return ds
