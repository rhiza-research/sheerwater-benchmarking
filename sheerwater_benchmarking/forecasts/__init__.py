"""Forecasting models for the Sheerwater benchmarking project."""
from .salient import (salient_era5, salient_blend, salient_blend_proc)

# Use __all__ to define what is part of the public API.
__all__ = [
    salient_blend_proc,
    salient_blend,
    salient_era5,
]
