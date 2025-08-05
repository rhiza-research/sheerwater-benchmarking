"""Test the metrics library."""

from sheerwater_benchmarking.metrics import grouped_metric_new, grouped_metric
from sheerwater_benchmarking.utils import start_remote
import matplotlib.pyplot as plt
import numpy as np


def test_single_comparison(forecast="ecmwf_ifs_er_debiased",
                           metric="mae",
                           variable="precip",
                           region="global",
                           lead="week3",
                           spatial=True):
    """Test a single comparison between the two functions."""

    print(f"Testing: {forecast} | {metric} | {variable} | {region} | {lead} | spatial={spatial}")

    # Run grouped_metric_new
    ds_new = grouped_metric_new(
        start_time="2016-01-01",
        end_time="2022-12-31",
        variable=variable,
        lead=lead,
        forecast=forecast,
        truth='era5',
        metric=metric,
        time_grouping=None,
        spatial=spatial,
        region=region,
        grid='global1_5',
        mask='lsm',
        recompute=False
    )

    # Run grouped_metric
    ds_old = grouped_metric(
        start_time="2016-01-01",
        end_time="2022-12-31",
        variable=variable,
        lead=lead,
        forecast=forecast,
        truth='era5',
        metric=metric,
        time_grouping=None,
        spatial=spatial,
        region=region,
        grid='global1_5',
        mask='lsm',
        recompute=False
    )

    # Compare results
    if ds_new is None and ds_old is None:
        print("Both functions returned None")
        return None, None

    if ds_new is None:
        print("Only grouped_metric_new returned None")
        return None, ds_old

    if ds_old is None:
        print("Only grouped_metric returned None")
        return ds_new, None

    # Both datasets exist
    new_data = ds_new[variable].compute()
    old_data = ds_old[variable].compute()

    print(f"New function result shape: {new_data.shape}")
    print(f"Old function result shape: {old_data.shape}")
    print(f"New function min/max: {float(new_data.min()):.6f} / {float(new_data.max()):.6f}")
    print(f"Old function min/max: {float(old_data.min()):.6f} / {float(old_data.max()):.6f}")

    # Compute difference
    try:
        diff = new_data - old_data
        diff_max = float(diff.max())
        diff_min = float(diff.min())
        diff_mean = float(diff.mean())
        diff_std = float(diff.std())

        print(f"Difference - min: {diff_min:.6f}, max: {diff_max:.6f}, mean: {diff_mean:.6f}, std: {diff_std:.6f}")

        if abs(diff_max) < 1e-10:
            print("✓ EXACT MATCH")
        elif abs(diff_max) < 1e-6:
            print("✓ CLOSE MATCH")
        else:
            print("✗ SIGNIFICANT DIFFERENCE")

    except Exception as e:
        print(f"Error computing difference: {e}")

    return ds_new, ds_old


def test_multiple_combinations():
    """Test multiple combinations of parameters."""

    test_cases = [
        # Basic tests
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "mae", "variable": "precip", "spatial": True},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "rmse", "variable": "precip", "spatial": True},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "bias", "variable": "precip", "spatial": True},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "acc", "variable": "precip", "spatial": True},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "pearson", "variable": "precip", "spatial": True},

        # Different forecasts
        {"forecast": "ecmwf_ifs_er", "metric": "mae", "variable": "precip", "spatial": True},
        {"forecast": "climatology_2015", "metric": "mae", "variable": "precip", "spatial": True},
        {"forecast": "fuxi", "metric": "mae", "variable": "precip", "spatial": True},

        # Different variables
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "mae", "variable": "tmp2m", "spatial": True},

        # Different regions
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "mae", "variable": "precip", "region": "africa", "spatial": True},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "mae", "variable": "precip", "region": "east_africa", "spatial": True},

        # Non-spatial tests
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "mae", "variable": "precip", "spatial": False},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "rmse", "variable": "precip", "spatial": False},

        # Categorical metrics
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "pod-1", "variable": "precip", "spatial": True},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "far-1", "variable": "precip", "spatial": True},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "ets-1", "variable": "precip", "spatial": True},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "heidke-1-5-10-20", "variable": "precip", "spatial": True},
    ]

    results = []

    for i, test_case in enumerate(test_cases):
        print(f"\n{'='*60}")
        print(f"Test case {i+1}/{len(test_cases)}")
        print(f"{'='*60}")

        # Set defaults
        test_case.setdefault("region", "global")
        test_case.setdefault("lead", "week3")

        ds_new, ds_old = test_single_comparison(**test_case)
        results.append({
            "test_case": i+1,
            "params": test_case,
            "new_result": ds_new is not None,
            "old_result": ds_old is not None
        })

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")

    successful_tests = sum(1 for r in results if r["new_result"] and r["old_result"])
    new_only = sum(1 for r in results if r["new_result"] and not r["old_result"])
    old_only = sum(1 for r in results if not r["new_result"] and r["old_result"])
    both_failed = sum(1 for r in results if not r["new_result"] and not r["old_result"])

    print(f"Total tests: {len(results)}")
    print(f"Both successful: {successful_tests}")
    print(f"Only new successful: {new_only}")
    print(f"Only old successful: {old_only}")
    print(f"Both failed: {both_failed}")


def plot_comparison(forecast="ecmwf_ifs_er_debiased",
                    metric="mae",
                    variable="precip",
                    region="global",
                    lead="week3",
                    spatial=True):
    """Create a plot comparing the results of both functions."""

    ds_new, ds_old = test_single_comparison(forecast, metric, variable, region, lead, spatial)

    if ds_new is None or ds_old is None:
        print("Cannot plot - one or both datasets are None")
        return

    new_data = ds_new[variable].compute()
    old_data = ds_old[variable].compute()

    # Create subplots
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    # Plot 1: New function result
    if spatial:
        new_data.plot(x='lon', ax=axes[0])
    else:
        axes[0].plot(new_data.values)
    axes[0].set_title(f'{metric.upper()} - New Function')

    # Plot 2: Old function result
    if spatial:
        old_data.plot(x='lon', ax=axes[1])
    else:
        axes[1].plot(old_data.values)
    axes[1].set_title(f'{metric.upper()} - Old Function')

    # Plot 3: Difference
    diff = new_data - old_data
    if spatial:
        diff.plot(x='lon', ax=axes[2])
    else:
        axes[2].plot(diff.values)
    axes[2].set_title(f'{metric.upper()} Difference (New - Old)')

    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    # Start remote cluster
    start_remote(remote_config='large_cluster')

    print("Starting simple metrics comparison test...")

    # Run multiple test combinations
    test_multiple_combinations()

    # Create a specific plot comparison
    print("\nCreating detailed plot comparison...")
    plot_comparison(forecast="ecmwf_ifs_er_debiased", metric="mae", variable="precip", spatial=True)
