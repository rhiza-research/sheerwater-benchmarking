"""Stage IV precipitation data product."""
import numpy as np
import xarray as xr
from dateutil import parser

from sheerwater_benchmarking.utils import apply_mask, cacheable, clip_region, dask_remote, regrid, roll_and_agg


@dask_remote
@cacheable(data_type='array',
           cache_args=['year', 'grid'],
           chunking={'lat': 300, 'lon': 300, 'time': 365})
def stage_iv_gridded(year, grid):
    """Regridded Stage IV precipitation data by year.

    Reads data from USGS Geo Data Portal.

    Data was stored in grib format, but was converted to zarr format.
    https://code.usgs.gov/wma/nhgf/geo-data-portal/stage_iv_precip
    We are using the zarr produced from that conversion.

    Data is available from 2002-present.
    """
    # Open the zarr store
    store = 'https://usgs.osn.mghpcc.org/mdmf/gdp/stageiv_combined.zarr'
    ds = xr.open_dataset(store, engine='zarr', chunks={})

    # Select the year
    ds = ds.sel(time=slice(f"{year}-01-01", f"{year}-12-31"))

    # Ensure consistent naming
    ds = ds.rename(
        {'Total_precipitation_surface_1_Hour_Accumulation': 'precip'},
    )

    # stage iv precipitation is measured in kg/m^2 and hourly
    # 1 kg/m^2 = 1 mm
    # to convert to our target units of mm/day, we are summing to daily values
    ds = ds.resample(time='1D').sum()
    ds.attrs.update(units='mm/day')

    # Fix coordinates and dimensions
    # Data is in polar stereographic projection with 2D lat/lon coordinates
    # Need to interpolate to a regular lat/lon grid before regridding

    # Get min/max bounds of the data
    lat_min = ds.latitude.min().values
    lat_max = ds.latitude.max().values
    lon_min = ds.longitude.min().values
    lon_max = ds.longitude.max().values

    # Create regular lat/lon coordinates at approximately the same resolution
    n_lat = ds.dims['y']
    n_lon = ds.dims['x']
    new_lats = np.linspace(lat_min, lat_max, n_lat)
    new_lons = np.linspace(lon_min, lon_max, n_lon)

    # Interpolate to the new regular grid
    ds = ds.interp(
        y=new_lats,
        x=new_lons,
        method='linear'
    )

    # Drop any existing coordinates
    ds = ds.reset_coords(drop=True)

    # Rename dimensions and assign final coordinates
    ds = ds.rename({'y': 'lat', 'x': 'lon'})
    ds = ds.assign_coords({
        'lat': ('lat', new_lats),
        'lon': ('lon', new_lons)
    })

    ds = ds.drop_vars(['crs'])

    # regrid
    ds = regrid(ds, grid, base='base180', method='conservative')

    return ds

@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['agg_days', 'grid'],
           chunking={'lat': 300, 'lon': 300, 'time': 365})
def stage_iv_rolled(start_time, end_time, agg_days, grid):
    """Stage IV rolled and aggregated."""
    years = range(parser.parse(start_time).year, parser.parse(end_time).year + 1)

    datasets = []
    for year in years:
        ds = stage_iv_gridded(year, grid, filepath_only=True)
        datasets.append(ds)

    # FIXME: Is this not working because of the disabled cache?
    # ds = xr.open_mfdataset(datasets,
    #                        engine='zarr',
    #                        parallel=True,
    #                        chunks={'lat': 300, 'lon': 300, 'time': 365})

    # Concatenate the datasets along the time dimension
    ds = xr.concat(datasets, dim='time')

    ds = roll_and_agg(ds, agg=agg_days, agg_col="time", agg_fn='mean')

    return ds

@dask_remote
@cacheable(data_type='array',
           timeseries='time',
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
