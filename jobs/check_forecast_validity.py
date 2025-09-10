# coding: utf-8

"""Test file for forecast data validation.

This testing should be run after forecast generation, to ensure forecast validity.
"""
import itertools
import dateparser

from sheerwater_benchmarking.metrics import get_datasource_fn
from sheerwater_benchmarking.utils import is_valid, dask_remote
from sheerwater_benchmarking import forecasts as fcst_mod
from sheerwater_benchmarking import climatology as baseline_mod


forecasts = fcst_mod.__forecasts__ + baseline_mod.__forecasts__
# Already checked salient
forecasts = [f for f in forecasts if f != 'salient']
leads = ["week1", "week2", "week3", "week4", "week5", "week6"]
variables = ["tmp2m", "precip"]
grids = ["global1_5", "global0_25"]
prob_types = ["deterministic", "probabilistic"]
regions = ['global', 'africa']

known_invalid = {
    'salient': {'region': 'global'},
    'perpp': {'prob_type': 'probabilistic'},
    'climatology_trend': {'prob_type': 'probabilistic'},
    'climatology_rolling': {'prob_type': 'probabilistic'},
}


def check_if_known_invalid(forecast, kwargs):
    """Check if the forecast parameters are invalid."""
    if forecast in known_invalid:
        for param, value in known_invalid[forecast].items():
            if value == kwargs[param]:
                print(kwargs[param], "is invalid for", forecast)
                return True
    return False


def has_weekly_forecast(start_time, end_time, times):
    """Check if the forecast has weekly data."""
    st = dateparser.parse(start_time)
    et = dateparser.parse(end_time)
    return len(times) >= (et - st).days // 7


@dask_remote
def test_forecast_validity(start_eval, end_eval):
    """Test the forecast data for validity."""
    invalid_params = []
    all_valid = True
    for forecast in forecasts:
        for variable, lead, prob_type, grid, region in itertools.product(variables, leads, prob_types, grids, regions):
            kwargs = {'variable': variable, 'lead': lead, 'prob_type': prob_type, 'grid': grid, 'region': region}
            print("Testing data for:", forecast, kwargs)
            if check_if_known_invalid(forecast, kwargs):
                print("Known invalid forecast. Skipping.")
                continue

            try:
                fcst_df = get_datasource_fn(forecast)(start_eval, end_eval, variable=variable, lead=lead,
                                                      prob_type=prob_type,
                                                      grid=grid, mask=None, region=region)
            except NotImplementedError:
                print("Threw not implemented. Skipping.")
                continue
            # Perform lazy checks for data validity
            if not len(fcst_df.coords) > 0:
                print("Invalid dataframe")
                all_valid = False
                invalid_params.append((forecast, kwargs))
                continue

            # Make sure the forecast has at least weekly data
            if not has_weekly_forecast(start_eval, end_eval, fcst_df.time):
                print("Invalid weekly forecast")
                all_valid = False
                invalid_params.append((forecast, kwargs))
                continue

            # Check that the forecast is valid in the region and mask specified
            if not is_valid(fcst_df, var=variable, mask=None, region=region, grid=grid, valid_threshold=0.99):
                print("Didn't pass valid check")
                all_valid = False
                invalid_params.append((forecast, kwargs))
                continue

    assert all_valid, f"Invalid forecasts: {invalid_params}"


if __name__ == "__main__":
    test_forecast_validity("2016-01-01", "2022-12-31", remote=True)
