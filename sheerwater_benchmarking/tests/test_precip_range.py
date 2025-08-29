"""Test that the precipitation range for the forecasters and reanalysis is reasonable."""
from sheerwater_benchmarking.forecasts import salient, ecmwf_ifs_er_debiased, ecmwf_ifs_er
from sheerwater_benchmarking.forecasts.climatology import climatology_2015, climatology_rolling, climatology_trend_2015
from sheerwater_benchmarking.reanalysis.era5 import era5


def test_precip_range():
    """Ensemble mean precipitation range for forecasters and reanalysis are reasonable."""
    start_time = "2016-01-01"
    end_time = "2016-12-31"
    variable = "precip"
    lead = "week2"
    prob_type = "deterministic"
    mask = "lsm"
    region = "global"

    for grid in ["global0_25", "global1_5"]:
        print(f"Grid: {grid}")
        dsr = era5(start_time, end_time, variable, agg_days=7, grid=grid, mask=mask, region=region)
        ds1 = salient(start_time, end_time, variable, lead=lead,
                      prob_type=prob_type, grid=grid, mask=mask, region=region)
        ds2 = ecmwf_ifs_er(start_time, end_time, variable, lead=lead,
                           prob_type=prob_type, grid=grid, mask=mask, region=region)
        ds3 = ecmwf_ifs_er_debiased(start_time, end_time, variable, lead=lead,
                                    prob_type=prob_type, grid=grid, mask=mask, region=region)
        ds4 = climatology_2015(start_time, end_time, variable, lead=lead,
                               prob_type=prob_type, grid=grid, mask=mask, region=region)
        ds5 = climatology_rolling(start_time, end_time, variable, lead=lead,
                                  prob_type=prob_type, grid=grid, mask=mask, region=region)
        ds6 = climatology_trend_2015(start_time, end_time, variable, lead=lead,
                                     prob_type=prob_type, grid=grid, mask=mask, region=region)
        valr = float(dsr['precip'].mean(skipna=True).compute())
        val1 = float(ds1['precip'].mean(skipna=True).compute())
        val2 = float(ds2['precip'].mean(skipna=True).compute())
        val3 = float(ds3['precip'].mean(skipna=True).compute())
        val4 = float(ds4['precip'].mean(skipna=True).compute())
        val5 = float(ds5['precip'].mean(skipna=True).compute())
        val6 = float(ds6['precip'].mean(skipna=True).compute())
        print("Reanalysis avg.", valr)
        print("Salient avg.", val1)
        print("ECMWF avg.", val2)
        print("ECMWF debiased avg.", val3)
        print("Climatology 2015 avg.", val4)
        print("Climatology rolling avg.", val5)
        print("Climatology trend 2015 avg.", val6)

        for val in [valr, val1, val2, val3, val4, val5, val6]:
            # 1.0 mm/day is a reasonable minimum
            assert val > 1.0
            # 3.0 mm/day is a reasonable maximum
            assert val < 3.0
            # Actual values are 1.5-2 mm/day
