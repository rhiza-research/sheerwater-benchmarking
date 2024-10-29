"""Baseline models for the Sheerwater benchmarking project."""
from .climatology import climatology_rolling, climatology_2015, climatology_plus_trend_2015

# Use __all__ to define what is part of the public API.
__all__ = ["climatology_rolling", "climatology_2015", "climatology_plus_trend_2015"]
