#!/usr/bin/env python
# coding: utf-8

"""Test file for ECMWF data validation.

This testing should be run ater data generation, to ensure data
validity.
"""
import itertools

from sheerwater_benchmarking.data import iri_ecmwf
from sheerwater_benchmarking.baselines import climatology


def test_iri_ecmwf(start_iri_ecmwf, end_iri_ecmwf):
    """Test the IRI ECMWF data for validity."""
    # Configurations of IRI ECMWF
    forecast_type = ["forecast", "reforecast"]
    run_type = ["average", "control"]
    forecast_type = ["forecast"]
    var = ["precip", "tmp2m"]
    run_type = ["average"]

    for f, v, r in itertools.product(forecast_type, var, run_type):
        print("Testing data for:", v, f, r)
        df = iri_ecmwf(start_iri_ecmwf, end_iri_ecmwf,
                       variable=v,
                       forecast_type=f,
                       run_type=r,
                       grid="global1_5")

        # Perform lazy checks for data validity
        assert len(df.coords) > 0

        # Check that there is majority non-null data for each start date
        assert (df.isnull().mean("start_date") < 0.5).all()


def test_climatology():
    """Test the IRI ECMWF data for validity."""
    # Configurations of IRI ECMWF
    ds = climatology(2000, 2020, "tmp2m", grid="global1_5")

    # Check that the data is of the correct shape
    assert ds.sizes["lat"] == 121
    assert ds.sizes["lon"] == 240
    assert len(ds.coords) == 2
