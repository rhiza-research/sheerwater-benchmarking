"""Test the metrics library."""
# flake8: noqa: E501

from sheerwater_benchmarking.metrics import grouped_metric_new, grouped_metric
from sheerwater_benchmarking.utils import start_remote, cacheable, dask_remote
import matplotlib.pyplot as plt


@dask_remote
@cacheable(data_type='array',
           cache_args=['start_time', 'end_time', 'variable', 'lead', 'forecast',
                       'truth', 'metric', 'time_grouping', 'spatial', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": -1},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, "time": 30}
               },
           },
           cache=True)
def grouped_metric_reference(start_time, end_time, variable, lead, forecast, truth,
                             metric, time_grouping=None, spatial=False, grid="global1_5",
                             mask='lsm', region='africa'):  # noqa
    """Stub function providing gold standard reference for testing."""
    pass


def test_single_comparison(forecast="ecmwf_ifs_er_debiased",
                           metric="mae",
                           variable="precip",
                           region="global",
                           lead="week3",
                           mask='lsm',
                           recompute=False,
                           spatial=True):  # noqa: E501
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
        mask=mask,
        grid='global1_5',
        recompute=recompute,
        force_overwrite=False,
    )
    if region in ds_new.dims and len(ds_new.region.values) > 1:
        ds_new = ds_new.sel(region=region)
    # ds_new = None

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
        mask=mask,
        grid='global1_5',
        recompute=False,
        retry_null_cache=True
    )

    # Compare results
    if ds_new is None and ds_old is None:
        print("Both functions returned None")
        return None, None, 0

    if ds_new is None:
        print("Only grouped_metric_new returned None")
        return None, ds_old, 1

    if ds_old is None:
        print("Only grouped_metric returned None")
        return ds_new, None, 2

    # Both datasets exist
    new_data = ds_new[variable].compute()
    old_data = ds_old[variable].compute()

    print(f"New function result shape: {new_data.shape}")
    print(f"Old function result shape: {old_data.shape}")
    print(
        f"New function min/max/mean: {float(new_data.min()):.6f} / {float(new_data.max()):.6f} / {float(new_data.mean()):.6f}")
    print(
        f"Old function min/max/mean: {float(old_data.min()):.6f} / {float(old_data.max()):.6f} / {float(old_data.mean()):.6f}")

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
            return ds_new, ds_old, 3
        elif abs(diff_max) < 0.007:
            print("✓ CLOSE MATCH")
            return ds_new, ds_old, 4
        else:
            print("✗ SIGNIFICANT DIFFERENCE")
            return ds_new, ds_old, 5

    except Exception as e:
        print(f"Error computing difference: {e}")
        raise e


def test_multiple_combinations():  # noqa: E501
    """Test multiple combinations of parameters."""
    test_cases = [
        # Our basic test, does lat-weighted averaging and masking globally
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "mae", "region": "global",
            "mask": 'lsm', "variable": "precip", "spatial": False},
        # Have to test with spatial = True because old code had a spatial weighting bug
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "mae", "region": "east_africa",
            "mask": 'lsm', "variable": "precip", "spatial": True},

        # Now test all the metrics
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "acc", "variable": "precip", "spatial": True},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "ets-5", "variable": "precip", "spatial": True},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "pod-5", "variable": "precip", "spatial": True},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "rmse", "variable": "precip", "spatial": True},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "bias", "variable": "precip", "spatial": True},
        # Test quantileCRPS, which can only be done with Salient in Africa
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "crps", "variable": "precip", "spatial": True},
        {"forecast": "salient", "metric": "crps", "variable": "precip", "spatial": True, 'region': 'africa'},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "smape", "variable": "precip", "spatial": True},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "mape", "variable": "precip", "spatial": False},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "seeps", "variable": "precip", "spatial": True},
        # Pearson only computed for week 3
        {"forecast": "ecmwf_ifs_er_debiased", "lead": "week3", "metric": "pearson", "variable": "precip", "spatial": True},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "heidke-1-5-10-20", "variable": "precip", "spatial": True},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "pod-10", "variable": "precip", "spatial": True},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "far-5", "variable": "precip", "spatial": True},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "frequencybias", "variable": "precip", "spatial": False},

        # Different forecasts
        {"forecast": "ecmwf_ifs_er", "metric": "mae", "variable": "precip", "spatial": False},
        {"forecast": "climatology_2015", "metric": "mae", "variable": "precip", "spatial": True},
        {"forecast": "fuxi", "metric": "mae", "variable": "precip", "spatial": True},

        # # Different variables
        # {"forecast": "ecmwf_ifs_er_debiased", "metric": "mae", "variable": "tmp2m", "spatial": True},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "acc", "variable": "precip", 'lead': 'week2', "spatial":  True},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "acc", "variable": "tmp2m", 'lead': 'week2', "spatial":  True},

        # Different regions(need to test with spatial=True because old code had a spatial weighting bug)
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "mae", "variable": "precip", "region": "africa", "spatial": True},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "mae", "variable": "precip", "region": "east_africa", "spatial": True},

        # Non-spatial tests, on coupled metrics.
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "mae", "variable": "precip", "spatial": False},
        {"forecast": "ecmwf_ifs_er_debiased", "metric": "acc", "variable": "precip", "spatial": False},
    ]

    results = []

    for i, test_case in enumerate(test_cases):
        print(f"\n{'='*60}")
        print(f"Test case {i+1}/{len(test_cases)}")
        print(f"{'='*60}")

        # Set defaults
        test_case.setdefault("region", "global")
        test_case.setdefault("lead", "week3")
        test_case.setdefault("recompute", ['global_statistic', 'grouped_metric_new'])
        test_case.setdefault("spatial", True)
        test_case.setdefault("mask", "lsm")

        ds_new, ds_old, result = test_single_comparison(**test_case)
        results.append({
            "test_case": i+1,
            "params": test_case,
            "result": result,
            "new_result": ds_new is not None,
            "old_result": ds_old is not None
        })

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")

    both_no_run = sum(1 for r in results if r["result"] == 0)
    new_failed = sum(1 for r in results if r["result"] == 1)
    old_failed = sum(1 for r in results if r["result"] == 2)
    exact_match = sum(1 for r in results if r["result"] == 3)
    close_match = sum(1 for r in results if r["result"] == 4)
    significant_difference = sum(1 for r in results if r["result"] == 5)

    print(f"Total tests: {len(results)}")
    print(f"Exact match: \t{exact_match} / {len(results)}, \t\t{exact_match/len(results)*100:.2f}%")
    print(f"Close match: \t{close_match} / {len(results)}, \t\t{close_match/len(results)*100:.2f}%")
    print(f"Sig. diff: \t{significant_difference} / {len(results)}, \t\t{significant_difference/len(results)*100:.2f}%")
    print(f"New failed: \t{new_failed} / {len(results)}, \t\t{new_failed/len(results)*100:.2f}%")
    print(f"Old failed: \t{old_failed} / {len(results)}, \t\t{old_failed/len(results)*100:.2f}%")
    print(f"Both no run: \t{both_no_run} / {len(results)}, \t\t{both_no_run/len(results)*100:.2f}%")

    failed_tests = [r['params'] for r in results if r["result"] in [0, 1, 5]]
    print(f"Failed tests: {failed_tests}")


def plot_comparison(forecast="ecmwf_ifs_er_debiased", metric="mae", variable="precip",
                    region="global", lead="week3", mask='lsm', spatial=True):
    """Create a plot comparing the results of both functions."""
    ds_new, ds_old = test_single_comparison(forecast, metric, variable, region, lead, mask, spatial)

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
    plot = False
    if plot:
        print("\nCreating detailed plot comparison...")
        plot_comparison(forecast="ecmwf_ifs_er_debiased", metric="mae", variable="precip", spatial=True)
        plot_comparison(forecast="climatology_2015", metric="bias", variable="tmp2m", spatial=True)
