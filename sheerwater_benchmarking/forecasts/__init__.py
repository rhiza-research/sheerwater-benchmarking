"""Forecasting models for the Sheerwater benchmarking project."""
from .abc import perpp
from .ecmwf_er import ecmwf_ifs_er, ecmwf_ifs_er_debiased
from .fuxi import fuxi
from .gefs import gefs
from .salient import salient

# Use __all__ to define what is part of the public API.
__all__ = ["ecmwf_abc_wb", "salient", "ecmwf_ifs_er_debiased", "ecmwf_ifs_er", "perpp", "fuxi", "gefs"]

# Define which are proper forecasts to be benchmarked
__forecasts__ = ["salient", "ecmwf_ifs_er_debiased", "ecmwf_ifs_er", "perpp", "fuxi", "gefs"]
