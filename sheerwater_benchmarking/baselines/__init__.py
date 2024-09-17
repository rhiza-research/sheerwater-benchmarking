"""Baseline models for the Sheerwater benchmarking project."""
from .climatology import climatology
from .ecmwf import iri_ecmwf, ecmwf_agg, ecmwf_rolled

# Use __all__ to define what is part of the public API.
__all__ = [
    climatology,
    iri_ecmwf,
    ecmwf_agg,
    ecmwf_rolled,
]
