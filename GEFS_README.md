All tests run on a cluster of 10 c3-standard-88 machines running in gcloud/coiled
See requirements.lock for current versions

Attempts/description
- Started with reading from the HTTP endpoint by running gefs_etl_https.py.
- Realized chunks must be set in the open_zarr to perform a large read because default chunks are too small
- Hit classic dask/encoding chunks misalignment error. drop encodings so to_zarr falls back to dask chunks
- HTTP error - maybe the chunks are too big?
- Smaller chunks make progress but keeps hitting the HTTP error with cloudflare - try switching to S3
- Switch to S3, run gefs_etl_s3.py. Now we get the checksum error
- Github discussion https://github.com/pydata/xarray/discussions/9938 indicates that this could be due chunk/sharding misalignment. Try to set shards as multiple of chunk for S3 try 2 - same error

To reproduce start a dask cluster, add S3 client key/secret from source coop, and change write location to a different datalake.
