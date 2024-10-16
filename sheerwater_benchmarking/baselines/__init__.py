"""Baseline models for the Sheerwater benchmarking project."""
from .climatology import climatology_agg, climatology_forecast

# Use __all__ to define what is part of the public API.
__all__ = [
    climatology_agg,
    climatology_forecast
]
