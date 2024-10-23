"""Forecasting models for the Sheerwater benchmarking project."""
from .salient import salient
from .ecmwf_er import ecmwf_agg, ecmwf_ifs_er, ecmwf_ifs_er_debaised

# Use __all__ to define what is part of the public API.
__all__ = ["ecmwf_agg", "salient", "ecmwf_ifs_er_debaised", "ecmwf_ifs_er"]
