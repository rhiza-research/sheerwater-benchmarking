#!/usr/bin/env python
"""Tooling for cache management."""

import argparse
import click
import gcsfs
import sys

parser = argparse.ArgumentParser()
parser.add_argument("-n", "--cache-name", type=str, required=True)
parser.add_argument("-v", "--value", type=str, help="The value in the cache key to delete", required=True)
args = parser.parse_args()

fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')

# Check if the cache name exists
cache_name = "gs://sheerwater-datalake/caches/" + args.cache_name

files = fs.ls(cache_name)
if len(files) == 0:
    raise ValueError(f"No cache found for {args.cache_name}")

files_to_delete = []
for f in files:
    if f.split('/')[-1].find(args.value) != -1:
        files_to_delete.append(f)

# Check with the user to see if this many files should be renamed
print("Deleting files:")
for i, f in enumerate(files_to_delete):
    print(f)


if click.confirm("Do you want to delete these files?"):
    for i, f in enumerate(files_to_delete):
        fs.rm(f, recursive=True)

    print()
    print("Files successfully deleted!")

sys.exit(1)
