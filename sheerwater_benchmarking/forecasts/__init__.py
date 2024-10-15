"""Forecasting models for the Sheerwater benchmarking project."""
from .salient import salient_forecast
from .ecmwf_er import ecmwf_agg, ecmwf_er_forecast

# Use __all__ to define what is part of the public API.
__all__ = [
    ecmwf_agg,
    ecmwf_er_forecast,
    salient_forecast,
]
