"""Generate Salient forecasts / reforecasts for all time."""
from itertools import product
from sheerwater_benchmarking.forecasts.salient import salient_blend


if __name__ == "__main__":
    vars = ["tmp2m", "precip"]
    timescales = ["sub-seasonal", "seasonal", "long-range"]
    grids = ['global1_5', 'global0_25']

    start_time = "2000-01-01"
    end_time = "2024-10-01"

    for var, timescale, grid in product(vars, timescales, grids):
        ds = salient_blend(start_time, end_time, var, timescale=timescale,
                           grid=grid,
                           remote=True, remote_name='regrid-salient', remote_config='xxlarge_cluster',
                           recompute=True, force_overwrite=True)
