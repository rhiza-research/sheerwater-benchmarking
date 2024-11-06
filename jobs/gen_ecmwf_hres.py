"""Generate Salient forecasts / reforecasts for all time."""
from itertools import product
from sheerwater_benchmarking.forecasts.ecmwf_hres import ecmwf_hres_rolled


if __name__ == "__main__":
    vars = ["tmp2m", "precip"]
    grids = ['global0_25', 'global1_5']

    start_time = "2016-01-01"
    end_time = "2022-12-31"

    for var, grid in product(vars, grids):
        ds = ecmwf_hres_rolled(start_time, end_time, var, agg=7, grid=grid,
                               remote=True, remote_name='geneveive', remote_config='xxlarge_cluster')
