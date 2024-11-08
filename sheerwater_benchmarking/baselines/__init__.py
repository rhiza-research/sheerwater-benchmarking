"""Baseline models for the Sheerwater benchmarking project."""
from .climatology import climatology_forecast, climatology_rolling,  \
    climatology_2015, climatology_trend_2015, \
    climatology_raw, climatology_agg_raw, climatology_abc, climatology_rolling_abc

# Use __all__ to define what is part of the public API.
__all__ = ["climatology_forecast", "climatology_2015", "climatology_trend_2015", "climatology_rolling",
           "climatology_raw", "climatology_abc", "climatology_rolling_abc", "climatology_agg_raw"]
