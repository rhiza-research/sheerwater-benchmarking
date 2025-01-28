"""Test lead-based target date fetching."""
import numpy as np

from sheerwater_benchmarking.utils import target_date_to_forecast_date, shift_forecast_date_to_target_date
from sheerwater_benchmarking.reanalysis import era5_rolled, era5
from sheerwater_benchmarking.forecasts import salient
from sheerwater_benchmarking.forecasts.salient import salient_blend
from sheerwater_benchmarking.forecasts.ecmwf_er import ifs_extended_range
from sheerwater_benchmarking.baselines import climatology_agg_raw, climatology_2015


def test_target_date_conversion():
    """Test the conversion of target dates to forecast dates."""
    start_date = "2020-01-14"
    end_date = "2020-01-31"

    # Test the target date to forecast date conversion
    fd_week1_start = target_date_to_forecast_date(start_date, "week1")
    fd_week1_end = target_date_to_forecast_date(end_date, "week1")
    assert fd_week1_start == start_date
    assert fd_week1_end == end_date

    # Should be shifted back by 7 days for week2
    fd_week2_start = target_date_to_forecast_date(start_date, "week2")
    fd_week2_end = target_date_to_forecast_date(end_date, "week2")
    assert fd_week2_start == "2020-01-07"
    assert fd_week2_end == "2020-01-24"

    # Should be shifted back by 14 days for weeks34
    fd_week34_start = target_date_to_forecast_date(start_date, "weeks34")
    fd_week34_end = target_date_to_forecast_date(end_date, "weeks34")
    assert fd_week34_start == "2019-12-31"
    assert fd_week34_end == "2020-01-17"

    # Ground truth data is already in "target date" format
    ds = era5(start_date, end_date, "tmp2m", agg_days=7, grid="global1_5", mask=None, region='global')
    dsr = era5_rolled(start_date, end_date, "tmp2m", agg_days=7, grid="global1_5")
    assert ds.equals(dsr)

    # Climatology data is already in "target date" format
    ds = climatology_2015(start_date, end_date, "tmp2m", "week3", grid="global1_5", mask=None, region='global')
    ds = ds.sel(time="2020-01-14")
    dsr = climatology_agg_raw("tmp2m", 1985, 2014, agg_days=7, grid="global1_5")
    dsr = dsr.sel(dayofyear="1904-01-14")
    # Align coordinates for comparison
    dsr = dsr.rename({"dayofyear": "time"})
    dsr['time'] = ds['time']
    assert ds.equals(dsr)

    # Test forecast conversion
    ds = salient(start_date, end_date, "tmp2m", "week2", grid="global1_5", mask=None, region='global')
    ds = ds.sel(time='2020-01-15')
    # Raw forecast
    dsr = salient_blend(fd_week2_start, fd_week2_end, "tmp2m", "sub-seasonal", grid="global1_5")
    dsr = dsr.sel(lead=2, quantiles=0.5, forecast_date="2020-01-08")
    dsr = dsr.drop("quantiles")
    dsr = dsr.rename({"forecast_date": "time"})
    dsr['time'] = ds['time']
    assert ds.equals(dsr)

    # Test forecast conversion
    ds = ifs_extended_range(fd_week2_start, fd_week2_end, "tmp2m", "forecast",
                            grid="global1_5", mask=None, region='global')
    # Get specific lead for week 2
    lead_shift = np.timedelta64(7, 'D')
    ds = ds.sel(lead_time=lead_shift)
    target_times = ds.start_date.values + ds.lead_time.values
    ds = shift_forecast_date_to_target_date(ds, 'start_date', lead='week2')
    ds = ds.rename({'start_date': 'time'})
    assert (ds.time.values == target_times).all()
