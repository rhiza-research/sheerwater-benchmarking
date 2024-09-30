"""Forecasting models for the Sheerwater benchmarking project."""
from .salient import salient_era5, salient_blend_raw, year_salient_blend_raw, salient_blend

# Use __all__ to define what is part of the public API.
__all__ = [
    salient_blend,
    salient_era5,
    salient_blend_raw,
    year_salient_blend_raw
]
