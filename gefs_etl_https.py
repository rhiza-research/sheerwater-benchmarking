#HTTP Version
import xarray as xr
import zarr

# For starting a dask (coiled) cluster
from sheerwater_benchmarking.utils import start_remote
start_remote(remote_config=['large_scheduler', 'large_cluster', 'xxlarge_node'])

store = "https://data.dynamical.org/noaa/gefs/forecast-35-day/latest.zarr"
ds = xr.open_zarr(store,
                  # Note - reading without chunks set here fails because the default chunk size is too small
                  # and the planner node runs out of memory trying to create the dask graph
                  # chunks of stored zarr = (1, 31, 64, 17, 16) ~2MB
                  # shards of stored zarr = (1, 31, 192, 374, 368) ~ 3GB

                  # Try to read in a strict shard subset to enable range-reads
                  #chunks={'init_time': 1, 'ensemble_member': 31, 'lead_time': 64, 'latitude': 374, 'longitude': 368}, # Try 1
                  chunks={'init_time': 1, 'ensemble_member': 31, 'lead_time': 16, 'latitude': 374, 'longitude': 368}, # Try 3
                  drop_variables=[
                      "categorical_freezing_rain_surface",
                      "categorical_freezing_rain_surface_avg",
                      "categorical_ice_pellets_surface",
                      "categorical_ice_pellets_surface_avg",
                      "categorical_rain_surface",
                      "categorical_rain_surface_avg",
                      "categorical_snow_surface",
                      "categorical_snow_surface_avg",
                      "geopotential_height_cloud_ceiling",
                      "percent_frozen_precipitation_surface",
                      "precipitable_water_atmosphere",
                      "precipitable_water_atmosphere_avg",
                      "pressure_reduced_to_mean_sea_level",
                      "pressure_reduced_to_mean_sea_level_avg",
                      "pressure_surface",
                      "pressure_surface_avg",
                      "precipitation_surface_avg",
                      "tmp2m_surface_avg",
                      "relative_humidity_2m",
                      "relative_humidity_2m_avg",
                      "total_cloud_cover_atmosphere",
                      "total_cloud_cover_atmosphere_avg",
                      "wind_u_100m",
                      "wind_u_10m",
                      "wind_u_10m_avg",
                      "wind_v_100m",
                      "wind_v_10m",
                      "wind_v_10m_avg",
                      "downward_short_wave_radiation_flux_surface_avg",
                      "downward_long_wave_radiation_flux_surface_avg",
                      "downward_short_wave_radiation_flux_surface",
                      "downward_long_wave_radiation_flux_surface",
                  ],
                  decode_timedelta=True)

# Clip out a single variable for the test
ds = ds[['precipitation_surface']]

# Try to manually reset encodings
ds = ds.drop_encoding() # Added for try 2
#chunks=(1000, 1, 1, 300, 300)
#shards=(1000, 1, 1, 600, 600)
#encoding = {
#    v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle),
#        "shards": shards,
#        "chunks":chunks}
#    for v in ds.data_vars
#}

encoding=None

# Write to zarr test location
ds.to_zarr(store="gcs://sheerwater-datalake/tmp/gefs-35.zarr", mode='w', encoding=encoding, zarr_format=3, consolidated=False)
