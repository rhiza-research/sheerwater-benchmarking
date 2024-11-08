"""Pulls ABC S2S forecasts from GCloud."""


import xarray as xr

from sheerwater_benchmarking.utils import (cacheable, dask_remote,
                                           get_variable, apply_mask, clip_region, regrid)


@dask_remote
@cacheable(data_type='array',
           timeseries='start_date',
           cache_args=['variable', 'lead'],
           cache=False)
def perpp_ecmwf_raw(start_time, end_time, variable, lead="weeks56"):  # noqa: ARG001
    """ABC function that returns data from GCP mirror.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        lead (str): The lead time of the forecast.
    """
    # Pull the Salient dataset
    var = get_variable(variable, 'abc')
    lead_id = {
        'weeks12': '12w',
        'weeks34': '34w',
        'weeks56': '56w',
    }[lead]
    filename = (f'gs://sheerwater-datalake/perpp_ecmwf/submodel_forecasts/'
                f'perpp_ecmwf-ef_yearsall_marginNone/global_{var}_1.5x1.5_{lead_id}/'
                f'global_{var}_1.5x1.5_{lead_id}-std_ecmwf.zarr')
    ds = xr.open_zarr(filename)
    ds = ds['pred'].to_dataset()
    ds = ds.rename(pred=variable)
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='start_date',
           cache_args=['variable', 'lead', 'grid'],
           chunking={"lat": 121, "lon": 240, "start_date": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'start_date': 30}
               },
           },
           auto_rechunk=False)
def perpp_ecmwf(start_time, end_time, variable, lead="weeks56", grid="global1_5"):
    """Processed ABC forecast files."""
    ds = perpp_ecmwf_raw(start_time, end_time, variable, lead=lead)

    # Regrid the data
    ds = regrid(ds, grid)
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask', 'region'])
def perpp(start_time, end_time, variable, lead, prob_type='deterministic',
          grid='global1_5', mask='lsm', region='global'):
    """Standard format forecast data for Persistance++ Model."""
    lead_params = {
        "weeks34": "weeks34",
        "weeks56": "weeks56",
    }
    lead_id = lead_params.get(lead, None)
    if lead_id is None:
        raise NotImplementedError(f"Lead {lead} not implemented for perpp.")

    ds = perpp_ecmwf(start_time, end_time, variable, lead=lead_id, grid=grid)
    if prob_type != 'deterministic':
        raise NotImplementedError("Probabilistic forecast not implemented for perpp.")
    ds = ds.assign_attrs(prob_type="deterministic")

    ds = ds.rename({'start_date': 'time'})

    # Apply masking
    ds = apply_mask(ds, mask, var=variable, grid=grid)
    # Clip to specified region
    ds = clip_region(ds, region=region)

    return ds
