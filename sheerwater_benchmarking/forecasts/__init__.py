"""Forecasting models for the Sheerwater benchmarking project."""
from .salient import salient
from .ecmwf_er import ecmwf_agg, ecmwf_er_deb, ecmwf_ifs_er

# Use __all__ to define what is part of the public API.
__all__ = ["ecmwf_agg", "salient", "ecmwf_er_deb", "ecmwf_ifs_er"]
