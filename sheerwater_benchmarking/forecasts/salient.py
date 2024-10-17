"""Pulls Salient Predictions S2S forecasts from the Salient API."""


import xarray as xr

from sheerwater_benchmarking.utils import (cacheable, dask_remote,
                                           get_variable, mask_and_clip, regrid)


@dask_remote
@cacheable(data_type='array',
           timeseries='forecast_date',
           cache_args=['variable', 'timescale'],
           cache=False)
def salient_blend_raw(start_time, end_time, variable,  # noqa: ARG001
                      timescale="sub-seasonal"):
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
           cache_args=['variable', 'grid', 'timescale', 'mask'],
           chunking={"lat": 300, "lon": 400, "forecast_date": 300, 'lead': 1, 'quantiles': 1},
           auto_rechunk=False)
def salient_blend(start_time, end_time, variable, timescale="sub-seasonal", grid="global0_25"):
    """Processed Salient forecast files."""
    ds = salient_blend_raw(start_time, end_time, variable, timescale=timescale)
    ds = ds.dropna('forecast_date', how='all')

    # Regrid the data
    ds = regrid(ds, grid)
    return ds


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
    timescale, lead_id = lead_params.get(lead, (None, None))
    if timescale is None:
        raise NotImplementedError(f"Lead {lead} not implemented for Salient.")

    ds = salient_blend(start_time, end_time, variable, timescale=timescale, grid=grid)
    ds = ds.sel(lead=lead_id)
    if prob_type == 'deterministic':
        # Get the median forecast
        ds = ds.sel(quantiles=0.5)
        ds['quantiles'] = -1
    ds = ds.rename({'quantiles': 'member'})
    ds = ds.rename({'forecast_date': 'time'})

    # Mask and clip the data
    ds = mask_and_clip(ds, variable, grid=grid, mask=mask, region=region)

    return ds
