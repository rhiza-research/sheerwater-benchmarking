"""Generate Salient forecasts / reforecasts for all time."""
from itertools import product
from sheerwater_benchmarking.forecasts import salient_blend_proc


if __name__ == "__main__":
    vars = ["tmp2m", "precip"]
    timescales = ["sub-seasonal", "seasonal", "long-range"]
    # grids = ['africa0_25', 'africa1_5']
    grids = ['africa1_5']

    start_time = "2000-01-01"
    end_time = "2024-10-01"

    for var, timescale, grid in product(vars, timescales, grids):
        ds = salient_blend_proc(start_time, end_time, var, timescale=timescale,
                                grid=grid,
                                recompute=True, force_overwrite=True,
                                remote=True)
