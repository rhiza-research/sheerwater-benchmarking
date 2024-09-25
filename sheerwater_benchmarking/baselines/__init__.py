"""Baseline models for the Sheerwater benchmarking project."""
from .climatology import climatology, climatology_standard_30yr
from .ecmwf import ecmwf_agg

# Use __all__ to define what is part of the public API.
__all__ = [
    climatology,
    climatology_standard_30yr,
    ecmwf_agg,
]
