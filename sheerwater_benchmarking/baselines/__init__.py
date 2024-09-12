"""Baseline models for the Sheerwater benchmarking project."""
from .climatology import climatology
from .ecmwf import (single_iri_ecmwf, iri_ecmwf, ecmwf_agg, single_iri_ecmwf_dense,
                    ecmwf_rolled)
