"""Re-run and re-cache the ECMWF aggregation and masking pipeline."""
from itertools import product
from sheerwater_benchmarking.forecasts.ecmwf_er import (ecmwf_agg, ecmwf_rolled, iri_ecmwf,
                                                        ecmwf_reforecast_bias, ecmwf_averaged_regrid)


if __name__ == "__main__":
    vars = ["tmp2m", "precip"]
    # vars = ["tmp2m"]
    aggs = [14, 7]
    # grids = ["global1_5"]
    grids = ["global0_25"]
    # grids = ["global0_25", "global1_5"]
    forecast_type = ["forecast", "reforecast"]
    # forecast_type = ["reforecast"]
    regions = ['global']
    masks = ["lsm"]

    start_time = "2015-05-14"
    end_time = "2023-06-30"

    UPDATE_IRI = False
    UPDATE_IRI_AVERAGED = True
    UPDATE_ROLLED = True
    UPDATE_AGG = False
    UPDATE_BIAS = False

    for var, ft in product(vars, forecast_type):
        if UPDATE_IRI:
            ds = iri_ecmwf(start_time, end_time, variable=var, forecast_type=ft,
                           run_type='perturbed',
                           grid='global1_5', verbose=True,
                           retry_null_cache=True,
                           remote=True,
                           remote_config={'name': 'update', 'n_workers': 10},
                           )

        for grid in grids:
            if UPDATE_IRI_AVERAGED:
                ds = ecmwf_averaged_regrid(start_time, end_time, variable=var, forecast_type=ft,
                                           grid=grid,
                                           recompute=True, force_overwrite=True,
                                           remote=True,
                                           remote_config={'name': 'genevieve',
                                                          'n_workers': 15, 'idle_timeout': '240 minutes'},
                                           )

            for agg in aggs:
                # Go back and update the earlier parts of the pipeline
                if UPDATE_ROLLED:
                    ds = ecmwf_rolled(start_time, end_time, variable=var, forecast_type=ft,
                                      grid=grid, agg=agg,
                                      recompute=True, force_overwrite=True,
                                      remote=True,
                                      remote_config={'name': 'genevieve',
                                                     'n_workers': 15, 'idle_timeout': '240 minutes'},
                                      )

                if UPDATE_BIAS:
                    if ft == "forecast":
                        ds = ecmwf_reforecast_bias(start_time, end_time, variable=var,
                                                   agg=agg, grid=grid,
                                                   recompute=False, force_overwrite=True,
                                                   remote=True, remote_config={'name': 'genevieve',
                                                                               'n_workers': 15,
                                                                               'idle_timeout': '240 minutes'})

                for region, mask in product(regions, masks):
                    if UPDATE_AGG:
                        ds = ecmwf_agg(start_time, end_time, variable=var, forecast_type=ft,
                                       grid=grid, agg=agg, mask=mask, region=region,
                                       recompute=True, force_overwrite=True,
                                       remote=True,
                                       remote_config={'name': 'update', 'n_workers': 10}
                                       )
