"""Re-run and re-cache the ECMWF aggregation and masking pipeline."""
from itertools import product
from sheerwater_benchmarking.baselines import ecmwf_agg
from sheerwater_benchmarking.utils import get_config
from sheerwater_benchmarking.baselines.ecmwf import ecmwf_rolled


if __name__ == "__main__":
    vars = ["tmp2m", "precip"]
    aggs = [14, 7, 1]
    masks = ["lsm"]
    grids = ["global1_5", "africa1_5"]
    forecast_type = ["forecast", "reforecast"]

    start_time = "2015-05-14"
    end_time = "2023-06-30"

    for var, agg, ft in product(vars, aggs, forecast_type):
        # Go back and update the earlier parts of the pipeline
        ds = ecmwf_rolled(start_time, end_time, variable=var, forecast_type=ft,
                          grid="global1_5", agg=agg,
                          recompute=True, force_overwrite=True,
                          remote=True,
                          remote_config=get_config('genevieve')
                          )

        for grid, mask in product(grids, masks):
            ds = ecmwf_agg(start_time, end_time, variable=var, forecast_type=ft,
                           grid=grid, agg=agg, mask=mask,
                           recompute=True, force_overwrite=True,
                           remote=True,
                           remote_config=get_config('genevieve')
                           )
