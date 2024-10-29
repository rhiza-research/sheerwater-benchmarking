"""Baseline models for the Sheerwater benchmarking project."""
from .climatology import rolling_climatology, climatology_2015, climatology_plus_trend_2015, \
    climatology_raw, climatology, climatology_standard_30yr, climatology_agg,  \
    climatology_rolling, climatology_rolling_agg, climatology_trend

# Use __all__ to define what is part of the public API.
__all__ = ["rolling_climatology", "climatology_2015", "climatology_plus_trend_2015",
           'climatology_raw', 'climatology', 'climatology_standard_30yr', 'climatology_agg',
           'climatology_rolling', 'climatology_rolling_agg', 'climatology_trend']
