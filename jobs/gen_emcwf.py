"""Re-run and re-cache the ECMWF aggregation and masking pipeline."""
from itertools import product
from sheerwater_benchmarking.forecasts.ecmwf_er import (ifs_extended_range_debiased,
                                                        ifs_extended_range, ecmwf_abc_wb)


if __name__ == "__main__":
    # vars = ["tmp2m", "precip"]
    # vars = ["precip", "tmp2m"]
    vars = ["precip"]
    # vars = ["tmp2m"]
    # aggs = [14, 7]
    # time_groups = ["weekly", "biweekly"]
    time_groups = ["daily"]
    # grids = ["global1_5", "global0_25"]
    # grids = ["global0_25"]
    grids = ["global1_5"]
    # grids = ["global0_25", "global1_5"]
    forecast_type = ["forecast", "reforecast"]
    # forecast_type = ["reforecast"]
    # forecast_type = ["forecast"]
    # run_types = ["average", "perturbed"]
    run_types = ["average"]
    # run_types = ["perturbed"]
    regions = ['global']
    masks = ["lsm"]

    start_time = "2015-05-14"
    end_time = "2023-06-30"

    UPDATE_IFS_ER_GRID = False
    UPDATE_BIAS = False
    UPDATE_AGG = True

    for var, ft, grid, time, rt in product(vars, forecast_type, grids, time_groups, run_types):
        if UPDATE_IFS_ER_GRID:
            if grid != "global0_25" or (grid == 'global0_25' and ft == "reforecast"):
                continue
            ds = ifs_extended_range(start_time, end_time, variable=var, forecast_type=ft,
                                    run_type=rt, time_group=time,
                                    grid=grid,
                                    remote=True,
                                    remote_config={'name': 'ecmwf-regrid2',
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
                                             remote_config='large_cluster'
                                             )

            # null_count = float(ds.isnull().sum().compute().to_array().values[0])
            # data_count = float(ds.count().compute().to_array().values[0])
            # if data_count == 0:
            #     print(f"{var} {ft} {grid} {time} {rt} has no data.")
            # else:
            #     null_frac = null_count / data_count
            #     print(f"{var} {ft} {grid} {time} {rt} has {null_frac*100} % missing values.")

        for region, mask in product(regions, masks):
            if UPDATE_AGG:
                ds = ecmwf_abc_wb(start_time, end_time, variable=var, forecast_type=ft,
                                  grid=grid, agg=time, mask=mask,
                                  remote=True,
                                  remote_config='large_cluster',
                                  remote_name='ecmwf_agg'
                                  )
