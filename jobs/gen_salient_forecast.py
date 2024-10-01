"""Generate Salient forecasts / reforecasts for all time."""
from itertools import product
from sheerwater_benchmarking.forecasts import salient_blend_raw


if __name__ == "__main__":
    vars = ["tmp2m", "precip"]
    timescales = ["sub-seasonal", "seasonal", "long-term"]

    start_time = "2000-01-01"
    end_time = "2024-09-01"

    for var, timescale in product(vars, timescales):
        # TODO: ensure that you run this on a node with large enough disk to download the
        # Salient forecasts locally, ~250 GB
        ds = salient_blend_raw(start_time, end_time, var, timescale=timescale)
