"""Stage IV precipitation data product."""
import xarray as xr
from dateutil import parser

from sheerwater_benchmarking.utils import apply_mask, cacheable, clip_region, dask_remote, regrid, roll_and_agg


@dask_remote
@cacheable(data_type='array',
           cache_args=['year', 'grid'],
           chunking={'lat': 300, 'lon': 300, 'time': 365})
def stage_iv_gridded(year, grid):
    """Regridded Stage IV precipitation data by year.

    Reads data from USGS OSN zarr store.
    Data is available from 2002-present.
    """
    # Open the zarr store
    store = 'https://usgs.osn.mghpcc.org/mdmf/gdp/stageiv_combined.zarr/'
    ds = xr.open_dataset(store, engine='zarr', chunks={})

    # Select the year
    ds = ds.sel(time=slice(f"{year}-01-01", f"{year}-12-31"))

    # Ensure consistent naming
    ds = ds.rename({'Total_precipitation_surface_1_Hour_Accumulation': 'precip'})

    # chirps is in "units": "mm/day"
    # stage iv is in "units": "kg m-2" and is measured hourly
    # 1 kg/m^2 = 1 mm, so just need to sum to daily values
    ds = ds.resample(time='1D').sum()
    ds.attrs.update(units='mm/day')

    # Fix coordinates and dimensions
    # Dimentions are currently stored in 2D (y,x) format (CRS:WGS84)
    # Coordinates:
    #     latitude   (y, x) float64 8MB dask.array<chunksize=(53, 68), meta=np.ndarray>
    #     longitude  (y, x) float64 8MB dask.array<chunksize=(53, 68), meta=np.ndarray>
    #   * time       (time) datetime64[ns] 6kB 2025-01-01 ... 2025-01-29T23:00:00

    # The data is WGS84 so no coordinate conversion is needed from (y,x) to (lat,lon)
    # Simple rename of the dimensions from (y,x) to (lat,lon)
    ds = ds.rename_dims({'y': 'lat', 'x': 'lon'})

    # We do need to convert to 1D arrays for lat and lon to match the expected input of the regrid function
    ds = ds.assign_coords({
        'lon': ('lon', ds.longitude[0,:].values),
        'lat': ('lat', ds.latitude[:,0].values)
    })

    # Drop the 2D coordinates and crs fields
    # The crs data is unneccesary as we are using WGS84 and already converted from y/x to lat/lon
    ds = ds.drop_vars(['latitude', 'longitude', 'crs'])

    # Coordinates:
    #   * time     (time) datetime64[ns] 232B 2025-01-01 2025-01-02 ... 2025-01-29
    #   * lon      (lon) float64 9kB -119.0 -119.0 -119.0 ... -80.81 -80.78 -80.75
    #   * lat      (lat) float64 7kB 23.12 23.15 23.18 23.21 ... 53.44 53.48 53.51

    # regrid
    ds = regrid(ds, grid, base='base180', method='conservative')

    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries=['time'],
           cache_args=['grid', 'agg_days'],
           chunking={'lat': 300, 'lon': 300, 'time': 365})
def stage_iv_rolled(start_time, end_time, agg_days, grid):
    """Stage IV rolled and aggregated."""
    years = range(parser.parse(start_time).year, parser.parse(end_time).year + 1)

    datasets = []
    for year in years:
        # NOTE: filepath_only is used by the cacheable decorator
        ds = stage_iv_gridded(year, grid, filepath_only=True)
        datasets.append(ds)

    ds = xr.open_mfdataset(datasets,
                           engine='zarr',
                           parallel=True,
                           chunks={'lat': 300, 'lon': 300, 'time': 365})

    ds = roll_and_agg(ds, agg=agg_days, agg_col="time", agg_fn='mean')

    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries=['time'],
           cache_args=['variable', 'agg_days', 'grid', 'mask', 'region'],
           cache=False)
def stage_iv(start_time, end_time, variable, agg_days, grid='global0_25', mask='lsm', region='global'):
    """Final Stage IV product."""
    if variable != 'precip':
        raise NotImplementedError("Only precip provided by Stage IV.")

    ds = stage_iv_rolled(start_time, end_time, agg_days, grid)

    # Apply masking
    ds = apply_mask(ds, mask, var=variable, grid=grid)

    # Clip to specified region
    ds = clip_region(ds, region=region)

    return ds
