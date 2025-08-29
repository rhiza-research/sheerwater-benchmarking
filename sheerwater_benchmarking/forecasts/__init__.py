"""Forecasting models for the Sheerwater benchmarking project."""
from .salient import salient
from .abc import perpp
from .ecmwf_er import ecmwf_ifs_er, ecmwf_ifs_er_debiased
from .fuxi import fuxi
from .graphcast import graphcast
from .gencast import gencast
from .climatology import climatology_rolling,  \
    climatology_2015, climatology_trend_2015, climatology_2020, \
    climatology_raw, climatology_agg_raw, seeps_wet_threshold, seeps_dry_fraction

# Use __all__ to define what is part of the public API.
__all__ = ["salient", "ecmwf_ifs_er_debiased", "ecmwf_ifs_er", "perpp", "fuxi", "graphcast", "gencast",
           "climatology_2015", "climatology_trend_2015", "climatology_2020", "climatology_rolling",
           "climatology_raw", "climatology_agg_raw", "seeps_wet_threshold", "seeps_dry_fraction"]

# Define which are proper forecasts to be benchmarked
__forecasts__ = ["salient", "ecmwf_ifs_er_debiased", "ecmwf_ifs_er", "perpp", "fuxi", "graphcast", "gencast",
                 "climatology_2015", "climatology_trend_2015", "climatology_2020", "climatology_rolling"]
