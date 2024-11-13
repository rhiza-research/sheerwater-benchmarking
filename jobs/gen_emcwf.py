"""Re-run and re-cache the ECMWF aggregation and masking pipeline."""
from itertools import product
from sheerwater_benchmarking.forecasts.ecmwf_er import (ecmwf_agg, ecmwf_rolled, iri_ecmwf,
                                                        ecmwf_averaged_regrid,
                                                        ifs_er_reforecast_bias, ifs_extended_range_debiased,
                                                        ifs_extended_range)


if __name__ == "__main__":
    vars = ["tmp2m", "precip"]
    # vars = ["precip", "tmp2m"]
    # vars = ["precip"]
    # vars = ["tmp2m"]
    aggs = [14, 7]
    # aggs = [14]
    time_groups = ['weekly', 'biweekly']
    # time_groups = ['biweekly']
    grids = ["global1_5", "global0_25"]
    # grids = ["global0_25"]
    # grids = ["global1_5"]
    # grids = ["global0_25", "global1_5"]
    forecast_type = ["forecast", "reforecast"]
    # forecast_type = ["reforecast"]
    # forecast_type = ["forecast"]
    run_types = ["average", "perturbed"]
    # run_types = ["average"]
    # run_types = ["perturbed"]
    regions = ['global']
    masks = ["lsm"]

    start_time = "2015-05-14"
    end_time = "2023-06-30"

    UPDATE_IFS_ER_GRID = True
    UPDATE_BIAS = False
    UPDATE_AGG = False

    for var, ft, grid, time, rt in product(vars, forecast_type, grids, time_groups, run_types):
        if UPDATE_IFS_ER_GRID:
            if grid != "global0_25" or (grid == 'global0_25' and ft == "reforecast"):
                continue
            ds = ifs_extended_range(start_time, end_time, variable=var, forecast_type=ft,
                                    run_type=rt, time_group=time,
                                    grid=grid,
                                    remote=True,
                                    remote_config={'name': 'ecmwf-regrid',
                                                   'worker_vm_types': 'c2-standard-16',
                                                   'n_workers': 35,
                                                   'idle_timeout': '240 minutes'})
        if UPDATE_BIAS:
            ds = ifs_extended_range_debiased(start_time, end_time, variable=var,
                                             run_type=rt, time_group=time,
                                             grid=grid,
                                             #  recompute=True, force_overwrite=True,
                                             remote=True,
                                             remote_name='genevieve',
                                             remote_config='xxlarge_cluster'
                                             )

        # for agg, region, mask in product(aggs, regions, masks):
        #     if UPDATE_AGG:
        #         ds = ecmwf_agg(start_time, end_time, variable=var, forecast_type=ft,
        #                        grid=grid, agg=agg, mask=mask, region=region,
        #                        recompute=True, force_overwrite=True,
        #                        remote=True,
        #                        remote_config={'name': 'update', 'n_workers': 10}
        #                        )
