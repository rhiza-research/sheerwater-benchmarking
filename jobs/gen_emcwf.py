"""Re-run and re-cache the ECMWF aggregation and masking pipeline."""
from itertools import product
from sheerwater_benchmarking.forecasts import ecmwf_agg


if __name__ == "__main__":
    vars = ["tmp2m", "precip"]
    aggs = [14, 7, 1]
    masks = ["lsm"]
    global_grids = ["global1_5"]
    # global_grids = ["global1_5"]
    # regional_grids = ["africa1_5"]
    regional_grids = ["us1_5"]
    forecast_type = ["forecast", "reforecast"]
    # forecast_type = ["forecast"]

    start_time = "2015-05-14"
    end_time = "2023-06-30"

    for var, agg, ft, global_grid in product(vars, aggs, forecast_type, global_grids):
        # Go back and update the earlier parts of the pipeline
        # ds = ecmwf_rolled(start_time, end_time, variable=var, forecast_type=ft,
        #                   grid=global_grid, agg=agg,
        #                   recompute=False, force_overwrite=False,
        #                   remote=True
        #                   )

        for rgrid, mask in product(regional_grids, masks):
            ds = ecmwf_agg(start_time, end_time, variable=var, forecast_type=ft,
                           grid=rgrid, agg=agg, mask=mask,
                           recompute=False, force_overwrite=False,
                           remote=True
                           )
