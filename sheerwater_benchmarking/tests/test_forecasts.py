#!/usr/bin/env python3
"""Test script for all forecasts functionality."""

import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
from sheerwater_benchmarking.utils import start_remote, get_datasource_fn


def test_function(function_name, test_params):
    """Test a single function with minimal parameters."""
    print(f"\n--- Testing {function_name} ---")

    try:
        # Get the function using get_datasource_fn
        fn = get_datasource_fn(function_name)

        # Test 1: Basic function call
        print(f"   Testing basic call...")
        
        # Extract start_time and end_time for positional arguments
        start_time = test_params['start_time']
        end_time = test_params['end_time']
        
        # Create kwargs without start_time and end_time
        kwargs = {k: v for k, v in test_params.items() if k not in ['start_time', 'end_time']}
        
        # Call function with positional arguments for start_time and end_time
        result = fn(start_time, end_time, **kwargs)

        if result is not None:
            print(f"   ✓ {function_name} succeeded")
            print(f"   ✓ Result type: {type(result)}")

            if hasattr(result, 'dims'):
                print(f"   ✓ Dataset shape: {result.dims}")
            if hasattr(result, 'data_vars'):
                print(f"   ✓ Variables: {list(result.data_vars.keys())}")
            if hasattr(result, 'coords'):
                print(f"   ✓ Coordinates: {list(result.coords.keys())}")

            return result
        else:
            print(f"   ⚠ {function_name} returned None")
            return None

    except Exception as e:
        print(f"   ✗ {function_name} failed: {e}")
        return None


def test_all_forecasts():
    """Test all available forecast functions."""
    print("\n=== Testing All Forecast Functions ===")

    # Define the forecasts we want to test
    forecasts_to_test = [
        'salient', 'ecmwf_ifs_er_debiased', 'ecmwf_ifs_er', 
        'fuxi', 'graphcast', 'gencast',
        'climatology_rolling', 'climatology_2015', 'climatology_trend_2015',
        'climatology_2020'
    ]

    # Test parameters for forecasts
    test_params = {
        'start_time': "2016-01-01",
        'end_time': "2022-12-31",
        'variable': "precip",
        'lead': 'weekly',
        'region': 'kenya',
        'grid': 'global1_5',
        'mask': 'lsm',
        'recompute': False
    }

    results = {}

    for forecast_name in forecasts_to_test:
        result = test_function(forecast_name, test_params)
        results[forecast_name] = result

    return results


def test_regions():
    """Test a few key regions with a simple forecast."""
    print("\n=== Testing Different Regions ===")

    regions_to_test = ['kenya', 'africa', 'conus', 'east_africa']

    try:
        # Use get_datasource_fn for ecmwf_ifs_er_debiased
        ecmwf_fn = get_datasource_fn('ecmwf_ifs_er_debiased')

        for region in regions_to_test:
            try:
                print(f"\n   Testing region: {region}")
                result = ecmwf_fn(
                    "2020-01-01",  # start_time as positional
                    "2020-01-31",  # end_time as positional
                    variable="precip",
                    lead='week1',
                    region=region,
                    grid='global1_5',
                    mask='lsm',
                    recompute=False
                )

                if result is not None:
                    print(f"   ✓ {region} succeeded - Shape: {result.dims}")
                else:
                    print(f"   ⚠ {region} returned None")

            except Exception as e:
                print(f"   ✗ {region} failed: {e}")

    except Exception as e:
        print(f"   ✗ Failed to test regions: {e}")


def test_lead_times():
    """Test different lead times with a simple forecast."""
    print("\n=== Testing Different Lead Times ===")

    leads_to_test = ['week1', 'week2', 'weeks12', 'month1']

    try:
        # Use get_datasource_fn for ecmwf_ifs_er_debiased
        ecmwf_fn = get_datasource_fn('ecmwf_ifs_er_debiased')

        for lead in leads_to_test:
            try:
                print(f"\n   Testing lead: {lead}")
                result = ecmwf_fn(
                    "2016-01-01",  
                    "2022-12-31",  
                    variable="precip",
                    lead=lead,
                    region='kenya',
                    grid='global1_5',
                    mask='lsm',
                    recompute=False
                )

                if result is not None:
                    print(f"   ✓ {lead} succeeded - Shape: {result.dims}")
                    if hasattr(result, 'lead_time'):
                        print(f"   ✓ Lead times: {result.lead_time.values}")
                else:
                    print(f"   ⚠ {lead} returned None")

            except Exception as e:
                print(f"   ✗ {lead} failed: {e}")

    except Exception as e:
        print(f"   ✗ Failed to test lead times: {e}")


def main():
    """Run all tests."""
    print("Starting Comprehensive Forecast Tests...\n")

    # Start remote cluster if needed
    try:
        start_remote(remote_config='large_cluster')
        print("✓ Remote cluster started")
    except Exception as e:
        print(f"⚠ Remote cluster issue: {e}")
        print("Continuing with local execution...")

    # Test all forecasts
    forecast_results = test_all_forecasts()

    # Test regions
    test_regions()

    # Test lead times
    test_lead_times()

    # Summary
    print("\n=== Test Summary ===")
    print(f"Forecasts tested: {len(forecast_results)}")

    successful_forecasts = sum(1 for r in forecast_results.values() if r is not None)

    print(f"Successful forecasts: {successful_forecasts}/{len(forecast_results)}")

    print("\n=== All Tests Completed ===")

    return forecast_results


if __name__ == "__main__":
    main()
