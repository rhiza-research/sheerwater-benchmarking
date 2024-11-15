"""Re-run and re-cache the ECMWF aggregation and masking pipeline."""
from itertools import product
from sheerwater_benchmarking.forecasts.ecmwf_er import (ifs_extended_range_debiased,
                                                        ifs_extended_range, ecmwf_abc_iri)


if __name__ == "__main__":
    vars = ["tmp2m", "precip"]
    # vars = ["precip", "tmp2m"]
    # vars = ["precip"]
    # vars = ["tmp2m"]
    aggs = [14, 7]
    # aggs = [14]
    # time_groups = ['weekly', 'biweekly']
    time_groups = ['biweekly']
    # grids = ["global1_5", "global0_25"]
    # grids = ["global0_25"]
    grids = ["global1_5"]
    # grids = ["global0_25", "global1_5"]
    # forecast_type = ["forecast", "reforecast"]
    # forecast_type = ["reforecast"]
    forecast_type = ["forecast"]
    run_types = ["average", "perturbed"]
    # run_types = ["average"]
    # run_types = ["perturbed"]
    regions = ['global']
    masks = ["lsm"]

    start_time = "2015-05-14"
    end_time = "2023-06-30"

    UPDATE_IRI = False
    UPDATE_IRI_AVERAGED = False
    UPDATE_ROLLED = False
    UPDATE_IFS_ER_GRID = False
    # UPDATE_IFS_ER_GRID = False
    # UPDATE_BIAS = True
    UPDATE_BIAS = True
    UPDATE_AGG = False

    for var, ft in product(vars, forecast_type):
        if UPDATE_IRI:
            ds = iri_ecmwf(start_time, end_time, variable=var, forecast_type=ft,
                           run_type='perturbed',
                           grid='global1_5', verbose=True,
                           retry_null_cache=True,
                           remote=False,
                           remote_config={'name': 'update', 'n_workers': 10},
                           )

        for grid in grids:
            if UPDATE_IRI_AVERAGED:
                ds = ecmwf_averaged_regrid(start_time, end_time, variable=var, forecast_type=ft,
                                           grid=grid,
                                           recompute=True, force_overwrite=True,
                                           remote=True,
                                           remote_config={'name': 'genevieve',
                                                          'n_workers': 25, 'idle_timeout': '240 minutes'},
                                           )
            for time, rt in product(time_groups, run_types):
                if UPDATE_IFS_ER_GRID:
                    ds = ifs_extended_range(start_time, end_time, variable=var, forecast_type=ft,
                                            run_type=rt, time_group=time,
                                            grid=grid,
                                            remote=True,
                                            recompute=True, force_overwrite=True,
                                            remote_config={'name': 'genevieve-run3',
                                                           'worker_vm_types': 'c2-standard-16',
                                                           'n_workers': 35,
                                                           'idle_timeout': '240 minutes'},
                                            )
                    # remote_config={'name': 'genevieve-run2',
                    #                'worker_vm_types': 'c2-standard-16',
                    #                'n_workers': 35,
                    #                'idle_timeout': '240 minutes'},
                    # )

        for region, mask in product(regions, masks):
            if UPDATE_AGG:
                ds = ecmwf_abc_iri(start_time, end_time, variable=var, forecast_type=ft,
                                   grid=grid, agg=time, mask=mask,
                                   remote=True,
                                   remote_config='large_cluster',
                                   remote_name='genevieve2'
                                   )
