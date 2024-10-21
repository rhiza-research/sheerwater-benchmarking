"""Baseline models for the Sheerwater benchmarking project."""
from .climatology import climatology_rolling_forecast, climatology_forecast_2015

# Use __all__ to define what is part of the public API.
__all__ = [
    climatology_forecast_2015,
    climatology_rolling_forecast
]
