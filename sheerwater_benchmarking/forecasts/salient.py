"""Pulls Salient Predictions S2S forecasts from the Salient API."""


import xarray as xr

from sheerwater_benchmarking.masks import land_sea_mask
from sheerwater_benchmarking.utils import (cacheable, dask_remote,
                                           apply_mask,
                                           get_variable, clip_region,
                                           regrid)


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
def salient_blend(start_time, end_time, variable, timescale="sub-seasonal",
                  grid="global0_25", mask='lsm'):
    """Processed Salient forecast files."""
    if grid == 'salient_africa0_25' and mask is not None:
        raise NotImplementedError('Masking not implemented for Salient native grid.')

    ds = salient_blend_raw(start_time, end_time, variable, timescale=timescale)
    ds = ds.dropna('forecast_date', how='all')

    # Regrid the data
    ds = regrid(ds, grid)

    if mask == "lsm":
        # Select variables and apply mask
        mask_ds = land_sea_mask(grid=grid).compute()
    elif mask is None:
        mask_ds = None
    else:
        raise NotImplementedError("Only land-sea or None mask is implemented.")

    ds = apply_mask(ds, mask_ds, variable)
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask', 'region'])
def salient(start_time, end_time, variable, lead, prob_type='deterministic',
            grid='africa0_25', mask='lsm', region='africa'):
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

    ds = salient_blend(start_time, end_time, variable, timescale=timescale,
                       grid=grid, mask=mask)
    ds = ds.sel(lead=lead_id)
    if prob_type == 'd':
        # Get the median forecast
        ds = ds.sel(quantiles=0.5)
        ds['quantiles'] = -1
    ds = ds.rename({'quantiles': 'member'})
    ds = ds.rename({'forecast_date': 'time'})

    # Clip to region
    ds = clip_region(ds, region)
    return ds
