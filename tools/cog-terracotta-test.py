# This is a test which
# (1) gets an xarray
# (2) saves that xarray as a COG (cloud-optmized geotiff)
# (3) builds a terracotta database for the COG
# (4) starts a terracotta server that runs off the mounted gcs and database
# (5) attempts to plot the data from the terracotta server

import io
from rasterio.io import MemoryFile
import gcsfs
from rasterio.enums import Resampling
import numpy as np
from sheerwater_benchmarking.reanalysis import era5
from google.cloud import storage
from sheerwater_benchmarking.utils.secrets import postgres_write_password

import terracotta as tc

if __name__ == '__main__':

    # Get a single day of era5 for our example
    #ds = era5("2000-01-01 00:00:00", "2000-02-01 00:00:00", "tmp2m")

    ## aggregate in time to get the average
    #ds = ds.mean('time')

    #ds = ds.assign_coords(lon=(((ds.lon + 180) % 360) - 180))
    #ds = ds.sortby(['lon'])

    ## Rename to x/y
    #ds = ds.rename({'lon': 'x', 'lat': 'y'})

    ## Try normalizing the values from 0-1
    ##ds['2m_temperature'] = ds['2m_temperature']/ds.max()
    ##ds = (ds/ds.max())
    #ds = (ds - ds.min()) / (ds.max() - ds.min())

    ## Write the CRS
    #ds.rio.write_crs("epsg:4326", inplace=True)
    #ds = ds.rio.reproject('EPSG:3857', resampling=Resampling.bilinear, nodata=np.nan)
    #ds.rio.write_crs("epsg:3857", inplace=True)

    ##ds.rio.to_raster(raster_path='./era5_tmp.tif', driver="COG")

    #with MemoryFile() as mem_dst:
    #    ds.rio.to_raster(mem_dst.name, driver="COG")

    #    storage_client = storage.Client()
    #    bucket = storage_client.bucket("sheerwater-datalake")
    #    blob = bucket.blob(f'rasters/era5_tmp.tif')
    #    blob.upload_from_file(mem_dst)

    # Connect to the terracotta driver
    tc.update_settings(SQL_USER="write", SQL_PASSWORD=postgres_write_password())
    driver = tc.get_driver("postgresql://sheerwater-benchmarking-postgres:5432/terracotta")

    try:
        print(driver.get_keys())
    except:
        # Create a metastor
        driver.create(['variable'])

    print(driver.get_datasets())

    # Insert the parameters.
    with driver.connect():
        driver.insert({'variable': 'tmp'}, 'era5_tmp.tif', override_path='/mnt/sheerwater-datalake/era5_tmp.tif')

    print(driver.get_datasets())

    # Now register this tif in terracotta


    # Move the raster to google cloud - there has to be a way to write it directly

    # Write this x/y array out to a COG in datalake
    #fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
    #cache_map = fs.get_mapper("gs://sheerwater-datalake/temp/era5_tmp.tif")
    #with open(cache_map) as f:
    #    ds.rio.to_raster(raster_path=mem_dst.name, driver="COG")

    #    with mem_dst.open() as dataset:
    #        with fs.open("gs://sheerwater-datalake/temp/era5_tmp.tif", mode='w') as f:
    #            f.write(dataset.read())
    #            f.flush()
