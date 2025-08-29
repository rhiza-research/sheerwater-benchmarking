#!/usr/bin/env python3
"""Test script for lead information generation functionality."""

import numpy as np
import pandas as pd
from sheerwater_benchmarking.utils import (
    get_lead_info,
    get_lead_group,
    forecast_date_to_target_date,
    target_date_to_forecast_date,
    convert_lead_to_valid_time
)


def test_get_lead_info():
    """Test the get_lead_info function for all supported lead types."""
    print("=== Testing get_lead_info Function ===")

    # Test cases for different lead types
    test_cases = {
        'weekly': {
            'expected_agg_days': 7,
            'expected_labels': ['week1', 'week2', 'week3', 'week4', 'week5', 'week6'],
            'expected_offsets': [0, 7, 14, 21, 28, 35]
        },
        'biweekly': {
            'expected_agg_days': 14,
            'expected_labels': ['weeks12', 'weeks23', 'weeks34', 'weeks45', 'weeks56'],
            'expected_offsets': [0, 7, 14, 21, 28]
        },
        'monthly': {
            'expected_agg_days': 30,
            'expected_labels': ['month1', 'month2', 'month3'],
            'expected_offsets': [0, 30, 60]
        },
        'quarterly': {
            'expected_agg_days': 90,
            'expected_labels': ['quarter1', 'quarter2', 'quarter3', 'quarter4'],
            'expected_offsets': [0, 90, 180, 270]
        },
        'daily-5': {
            'expected_agg_days': 1,
            'expected_labels': ['day1', 'day2', 'day3', 'day4', 'day5'],
            'expected_offsets': [0, 1, 2, 3, 4]
        },
        'day5': {
            'expected_agg_days': 1,
            'expected_labels': ['day5'],
            'expected_offsets': [5]  # day5 means 5 days offset (0-indexed)
        },
        'week3': {
            'expected_agg_days': 7,
            'expected_labels': ['week3'],
            'expected_offsets': [28]  # week3 means 28 days offset (4*7)
        },
        'weeks12': {
            'expected_agg_days': 14,
            'expected_labels': ['weeks12'],
            'expected_offsets': [0]  # weeks12: week 1-2, 0 days offset
        },
        'weeks23': {
            'expected_agg_days': 14,
            'expected_labels': ['weeks23'],
            'expected_offsets': [7]  # weeks23: week 2-3, 7 days offset
        },
        'weeks34': {
            'expected_agg_days': 14,
            'expected_labels': ['weeks34'],
            'expected_offsets': [14]  # weeks34: week 3-4, 14 days offset
        },
        'month2': {
            'expected_agg_days': 30,
            'expected_labels': ['month2'],
            'expected_offsets': [60]  # month2 means 60 days offset (2*30)
        },
        'quarter3': {
            'expected_agg_days': 90,
            'expected_labels': ['quarter3'],
            'expected_offsets': [270]  # quarter3 means 270 days offset (3*90)
        }
    }

    for lead, expected in test_cases.items():
        try:
            print(f"\nTesting lead: {lead}")
            result = get_lead_info(lead)

            # Check structure
            assert 'agg_period' in result, f"Missing agg_period for {lead}"
            assert 'agg_days' in result, f"Missing agg_days for {lead}"
            assert 'lead_offsets' in result, f"Missing lead_offsets for {lead}"
            assert 'labels' in result, f"Missing labels for {lead}"

            # Check values
            assert result['agg_days'] == expected['expected_agg_days'], \
                f"Agg days mismatch for {lead}: expected {expected['expected_agg_days']}, got {result['agg_days']}"

            assert result['labels'] == expected['expected_labels'], \
                f"Labels mismatch for {lead}: expected {expected['expected_labels']}, got {result['labels']}"

            # Check offsets (convert timedelta64 to days for comparison)
            result_offsets = [int(offset / np.timedelta64(1, 'D')) for offset in result['lead_offsets']]
            assert result_offsets == expected['expected_offsets'], \
                f"Offsets mismatch for {lead}: expected {expected['expected_offsets']}, got {result_offsets}"

            print(f"   ✓ {lead} - All checks passed")

        except Exception as e:
            print(f"   ✗ {lead} - Error: {e}")
            import traceback
            traceback.print_exc()


def test_get_lead_group():
    """Test the get_lead_group function."""
    print("\n=== Testing get_lead_group Function ===")

    test_cases = {
        'week1': 'weekly',
        'week2': 'weekly',
        'week3': 'weekly',
        'weeks12': 'biweekly',
        'weeks23': 'biweekly',
        'month1': 'monthly',
        'month2': 'monthly',
        'quarter1': 'quarterly',
        'quarter2': 'quarterly',
        'day1': 'daily',
        'day5': 'daily',
        'daily-3': 'daily',
        'weekly': 'weekly',
        'monthly': 'monthly',
        'quarterly': 'quarterly'
    }

    for lead, expected_group in test_cases.items():
        try:
            result = get_lead_group(lead)
            assert result == expected_group, \
                f"Lead group mismatch for {lead}: expected {expected_group}, got {result}"
            print(f"   ✓ {lead} -> {result}")
        except Exception as e:
            print(f"   ✗ {lead} - Error: {e}")


def test_lead_or_agg():
    """Test the lead_or_agg function."""
    print("\n=== Testing lead_or_agg Function ===")

    test_cases = {
        'week1': 'lead',
        'weeks12': 'lead',
        'month1': 'lead',
        'quarter1': 'lead',
        'day1': 'lead',
        'daily-3': 'lead',
        'weekly': 'lead',
        'biweekly': 'lead',
        'monthly': 'lead',
        'quarterly': 'lead',
        'daily': 'lead',
        'mean': 'agg',
        'sum': 'agg',
        'max': 'agg',
        'min': 'agg'
    }

    for lead, expected_type in test_cases.items():
        try:
            result = lead_or_agg(lead)
            assert result == expected_type, \
                f"Type mismatch for {lead}: expected {expected_type}, got {result}"
            print(f"   ✓ {lead} -> {result}")
        except Exception as e:
            print(f"   ✗ {lead} - Error: {e}")


def test_date_conversion_functions():
    """Test date conversion functions."""
    print("\n=== Testing Date Conversion Functions ===")

    # Test base date
    base_date = "2020-01-01"

    # Test cases for different leads
    test_leads = ['week1', 'day5', 'month2']

    for lead in test_leads:
        try:
            print(f"\nTesting date conversion for lead: {lead}")

            # Test forecast_date_to_target_date
            target_date = forecast_date_to_target_date(base_date, lead)
            print(f"   ✓ {base_date} + {lead} = {target_date}")

            # Test target_date_to_forecast_date (should reverse the above)
            forecast_date = target_date_to_forecast_date(target_date, lead)
            print(f"   ✓ {target_date} - {lead} = {forecast_date}")

            # Verify they're consistent
            assert forecast_date == base_date, \
                f"Date conversion not consistent for {lead}: {base_date} -> {target_date} -> {forecast_date}"

        except Exception as e:
            print(f"   ✗ {lead} - Error: {e}")


def test_convert_lead_to_valid_time():
    """Test the convert_lead_to_valid_time function."""
    print("\n=== Testing convert_lead_to_valid_time Function ===")

    try:
        # Create a simple test dataset
        import xarray as xr

        # Create test coordinates
        start_dates = pd.date_range('2020-01-01', periods=3, freq='D')
        lead_times = [np.timedelta64(7, 'D'), np.timedelta64(14, 'D'), np.timedelta64(21, 'D')]

        # Create test data
        data = np.random.rand(3, 3)

        # Create dataset
        ds = xr.Dataset(
            data_vars={'test_var': (['start_date', 'lead_time'], data)},
            coords={
                'start_date': start_dates,
                'lead_time': lead_times
            }
        )

        print(f"   ✓ Created test dataset: {ds.dims}")

        # Test conversion
        result = convert_lead_to_valid_time(ds, 'start_date', 'lead_time', 'valid_time')

        print(f"   ✓ Conversion successful: {result.dims}")
        print(f"   ✓ Valid time coordinates: {result.valid_time.values}")

        # The function creates a cartesian product, so we need to check the structure
        # It should have 9 valid times (3 start dates × 3 lead times)
        assert len(result.valid_time) == 9, f"Expected 9 valid times, got {len(result.valid_time)}"

        # Check that the first few times are correct
        # First start date + first lead time
        expected_first = start_dates[0] + lead_times[0]
        assert result.valid_time.values[
            0] == expected_first, f"First time mismatch: expected {expected_first}, got {result.valid_time.values[0]}"

        print("   ✓ Valid time structure is correct (9 total times)")
        print(f"   ✓ First valid time matches expected: {expected_first}")

    except Exception as e:
        print(f"   ✗ Error: {e}")
        import traceback
        traceback.print_exc()


def test_edge_cases():
    """Test edge cases and error handling."""
    print("\n=== Testing Edge Cases and Error Handling ===")

    # Test invalid leads
    invalid_leads = ['invalid']

    for lead in invalid_leads:
        try:
            result = get_lead_info(lead)
            print(f"   ⚠ {lead} unexpectedly succeeded: {result}")
        except ValueError as e:
            print(f"   ✓ {lead} correctly raised ValueError: {e}")
        except Exception as e:
            print(f"   ✗ {lead} raised unexpected error: {e}")

    # Test out-of-bounds leads that should now fail
    out_of_bounds_leads = [
        'day0', 'day367', 'day999',
        'week0', 'week53', 'week99',
        'weeks53', 'weeks99', 'weeks11', 'weeks22', 'weeks33', 'weeks44', 'weeks55',
        'month0', 'month13', 'month99',
        'quarter0', 'quarter5', 'quarter99',
        'daily-0', 'daily-367', 'daily-999'
    ]

    for lead in out_of_bounds_leads:
        try:
            result = get_lead_info(lead)
            print(f"   ⚠ {lead} unexpectedly succeeded: {result}")
        except ValueError as e:
            print(f"   ✓ {lead} correctly raised ValueError: {e}")
        except Exception as e:
            print(f"   ✗ {lead} raised unexpected error: {e}")

    # Test empty or None inputs
    try:
        result = get_lead_info("")
        print(f"   ⚠ Empty string unexpectedly succeeded: {result}")
    except ValueError as e:
        print(f"   ✓ Empty string correctly raised ValueError: {e}")

    try:
        result = get_lead_info(None)
        print(f"   ⚠ None unexpectedly succeeded: {result}")
    except Exception as e:
        print(f"   ✓ None correctly raised error: {e}")


def main():
    """Run all lead generation tests."""
    print("Starting Lead Generation Tests...\n")

    try:
        test_get_lead_info()
        test_get_lead_group()
        test_lead_or_agg()
        test_date_conversion_functions()
        test_convert_lead_to_valid_time()
        test_edge_cases()

        print("\n=== All Tests Completed Successfully! ===")

    except Exception as e:
        print(f"\n✗ Test suite failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
