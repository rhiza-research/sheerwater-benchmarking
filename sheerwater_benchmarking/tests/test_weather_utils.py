"""Simple tests for the weather utility functions."""

import pytest
import numpy as np
import xarray as xr

from sheerwater_benchmarking.utils.weather_utils import (
    forecast, get_leads, get_forecast, FORECAST_REGISTRY
)


def test_get_leads_weekly():
    """Test weekly lead generation."""
    leads, agg_period = get_leads('weekly')
    assert leads == ['week1', 'week2', 'week3', 'week4', 'week5', 'week6']
    assert agg_period == np.timedelta64(7, 'D')


def test_get_leads_daily():
    """Test daily lead generation."""
    leads, agg_period = get_leads('daily-3')
    assert leads == ['day1', 'day2', 'day3']
    assert agg_period == np.timedelta64(3, 'D')


def test_get_leads_single():
    """Test single lead generation."""
    leads, agg_period = get_leads('day5')
    assert leads == ['day5']
    assert agg_period == np.timedelta64(1, 'D')


def test_get_leads_invalid():
    """Test invalid lead raises error."""
    with pytest.raises(ValueError):
        get_leads('invalid')


def test_forecast_decorator():
    """Test basic forecast decorator functionality."""
    FORECAST_REGISTRY.clear()

    @forecast
    def simple_forecast(*args, **kwargs):
        data = np.random.random((2, 2))
        ds = xr.Dataset({'data': (['lat', 'lon'], data)},
                        coords={'lat': [0, 1], 'lon': [0, 1]})
        return ds

    # Function should be registered immediately when decorated
    assert 'simple_forecast' in FORECAST_REGISTRY

    # Test with weekly leads
    result = simple_forecast(lead='weekly')
    assert 'lead_time' in result.coords
    assert len(result.lead_time) == 6
    assert result.attrs['agg_period'] == np.timedelta64(7, 'D')


def test_forecast_single_lead():
    """Test forecast decorator with single lead."""
    FORECAST_REGISTRY.clear()

    @forecast
    def single_forecast(*args, **kwargs):
        data = np.random.random((2, 2))
        ds = xr.Dataset({'data': (['lat', 'lon'], data)},
                        coords={'lat': [0, 1], 'lon': [0, 1]})
        return ds

    result = single_forecast(lead='day3')
    assert 'lead_time' in result.coords
    assert len(result.lead_time) == 1
    assert result.lead_time.values[0] == 'day3'


def test_forecast_error_handling():
    """Test forecast decorator error handling."""
    FORECAST_REGISTRY.clear()

    @forecast
    def error_forecast(*args, **kwargs):
        lead = kwargs.get('lead')
        if lead == 'week1':
            raise NotImplementedError("Week1 not supported")
        data = np.random.random((2, 2))
        ds = xr.Dataset({'data': (['lat', 'lon'], data)},
                        coords={'lat': [0, 1], 'lon': [0, 1]})
        return ds

    # Should skip week1 and continue with others
    result = error_forecast(lead='weekly')
    assert len(result.lead_time) == 5  # week1 excluded
    assert 'week1' not in result.lead_time.values


def test_get_forecast():
    """Test getting forecast from registry."""
    FORECAST_REGISTRY.clear()

    @forecast
    def test_func(*args, **kwargs):
        return xr.Dataset()

    # Function should be registered immediately when decorated
    assert 'test_func' in FORECAST_REGISTRY

    # Get the function from registry
    func = get_forecast('test_func')
    assert func.__name__ == 'test_func'

    # Test non-existent forecast
    with pytest.raises(KeyError):
        get_forecast('nonexistent')


def test_all_forecasts_registered():
    """Test that all forecast functions are properly registered."""
    # Clear registry to start fresh
    FORECAST_REGISTRY.clear()

    # Import all modules that contain @forecast decorated functions
    # This will trigger the decorators and register the functions
    import sheerwater_benchmarking.forecasts.salient  # noqa: F401
    import sheerwater_benchmarking.forecasts.fuxi  # noqa: F401
    import sheerwater_benchmarking.forecasts.gencast  # noqa: F401
    import sheerwater_benchmarking.forecasts.ecmwf_er  # noqa: F401
    import sheerwater_benchmarking.forecasts.graphcast  # noqa: F401
    import sheerwater_benchmarking.baselines.climatology  # noqa: F401

    # Expected forecast function names based on the codebase
    expected_forecasts = {
        'salient',
        'fuxi',
        'ecmwf_ifs_er',
        'ecmwf_ifs_er_debiased',
        'gencast',
        'graphcast',
        'climatology_2015',
        'climatology_2020',
        'climatology_trend_2015',
        'climatology_rolling',
    }

    # Check that all expected forecasts are registered
    missing_forecasts = expected_forecasts - set(FORECAST_REGISTRY.keys())
    extra_forecasts = set(FORECAST_REGISTRY.keys()) - expected_forecasts

    # Report any issues
    if missing_forecasts:
        print(f"Missing forecasts: {missing_forecasts}")
    if extra_forecasts:
        print(f"Unexpected forecasts: {extra_forecasts}")

    # All expected forecasts should be present
    assert len(missing_forecasts) == 0, f"Missing forecasts: {missing_forecasts}"

    # Verify each forecast function can be retrieved
    for forecast_name in expected_forecasts:
        func = get_forecast(forecast_name)
        assert func is not None, f"Forecast {forecast_name} is None"
        assert hasattr(func, '__name__'), f"Forecast {forecast_name} has no __name__"
        assert func.__name__ == forecast_name, f"Forecast {forecast_name} name mismatch"

    print(f"✅ All {len(expected_forecasts)} forecast functions are properly registered!")
    print(f"Registered forecasts: {sorted(FORECAST_REGISTRY.keys())}")
