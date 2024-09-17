"""Forecasting models for the Sheerwater benchmarking project."""
from .salient import salient_forecast

# Use __all__ to define what is part of the public API.
__all__ = [
    salient_forecast,
]
