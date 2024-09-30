"""Forecasting models for the Sheerwater benchmarking project."""
from .salient import salient_era5, salient_blend_raw, year_salient_blend_raw

# Use __all__ to define what is part of the public API.
__all__ = [
    salient_era5,
    salient_blend_raw,
    year_salient_blend_raw
]
