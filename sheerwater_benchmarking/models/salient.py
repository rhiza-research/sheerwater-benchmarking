"""Pulls Saleint Predictions S2S forecasts from the Salient API."""
import numpy as np
import xarray as xr
import salientsdk as sk

from sheerwater_benchmarking.utils import dask_remote, cacheable, salient_auth
from sheerwater_benchmarking.utils.general_utils import get_dates
from sheerwater_benchmarking.utils.model_utils import get_salient_loc


@salient_auth
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'grid'],
           chunking={"lat": 292, "lon": 396, "time": 500},
           auto_rechunk=False)
def salient_blend_raw(start_time, end_time, variable, grid="africa0_25", verbose=False):
    """Fetches ground truth data from Saleint's SDK and applies aggregation and masking .

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        grid (str): The grid resolution to fetch the data at. One of:
            - africa0_25: 0.25 degree African grid
    """

    # Fetch the data from Salient
    loc = get_salient_loc(grid)
    var_name = {'tmp2m': 'temp', 'precip': 'precip'}[variable]

    # Fetch and load the data
    # date_range = pd.date_range(start=np.datetime64(start_time), end=np.datetime64(end_time),
    #                            freq="W").strftime("%Y-%m-%d").tolist()
    target_dates = get_dates(start_time, end_time, stride="week", return_string=True)

    fcst = sk.forecast_timeseries(
        loc=loc,
        variable=var_name,
        field="vals",
        date=target_dates,  # to request multiple forecast dates
        timescale=timescale,
        model="blend",
        # reference_clim="30_yr",  # this is the climatology used by data_timeseries
        version="v8",
        verbose=False,
        force=True,
        strict=False,  # There is missing data in 2020.  Work around it.
    )

    # Because we requested multiple forecast dates and models, the result is a vector of file names
    print(fcst)

    data = sk.data_timeseries(
        loc=loc,
        variable=variable,
        field="vals",
        start=np.datetime64(start_time),
        end=np.datetime64(end_time),
        frequency="daily",
        verbose=verbose,
        force=True,
    )
    return xr.load_dataset(data)
