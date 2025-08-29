"""Baseline models for the Sheerwater benchmarking project."""
from .climatology import climatology_rolling,  \
    climatology_2015, climatology_trend_2015, climatology_2020, \
    climatology_raw, climatology_agg_raw, seeps_wet_threshold, seeps_dry_fraction

# Use __all__ to define what is part of the public API.
__all__ = ["climatology_2015", "climatology_trend_2015", "climatology_2020", "climatology_rolling",
           "climatology_raw", "climatology_agg_raw", "seeps_wet_threshold", "seeps_dry_fraction"]

# Define which are proper forecasts to be benchmarked
__forecasts__ = ["climatology_2015", "climatology_trend_2015", "climatology_2020", "climatology_rolling"]
