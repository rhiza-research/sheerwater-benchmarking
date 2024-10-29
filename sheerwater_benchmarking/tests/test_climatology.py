#!/usr/bin/env python
# coding: utf-8

"""Test file for climatology data validation."""

from sheerwater_benchmarking.baselines import climatology, climatology_standard_30yr


def test_climatology():
    """Test the climatology data for validity."""
    # Configurations of climatology
    first_year = 1991
    last_year = 2020
    variable = "tmp2m"

    ds = climatology(first_year, last_year, variable, grid="global1_5", mask="lsm")
    dsp = climatology_standard_30yr(variable, grid="global1_5", mask="lsm")

    assert ds.sizes["doy"] == 366
    assert dsp.sizes["doy"] == 366
