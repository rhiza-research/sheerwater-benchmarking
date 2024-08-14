#!/usr/bin/env python

import argparse
import click
import gcsfs
import sys

parser = argparse.ArgumentParser()
parser.add_argument("-n", "--cache-name", type=str, required=True)
parser.add_argument("-ov", "--old-value", type=str, help="The old value of the cache to rename", required=True)
parser.add_argument("-nv", "--new-value", type=str, help="The new value of the cache to change to", required=True)
args = parser.parse_args()

fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')

# Check if the cache name exists
cache_name = "gs://sheerwater-datalake/caches/" + args.cache_name

files = fs.ls(cache_name)
if len(files) == 0:
    raise ValueError(f"No cache found for {args.cache_name}")

files_to_rename = []
for f in files:
    if f.split('/')[-1].find(args.old_value) != -1:
        files_to_rename.append(f)

new_names = []
for f in files_to_rename:
    beginning = f.split('/')[:-1]
    end = f.split('/')[-1]
    end = end.replace(args.old_value, args.new_value, 1)
    new_names.append('/'.join(beginning) + '/' + end)

# Check with the user to see if this many files should be renamed
print("Renaming files:")
for i, f in enumerate(files_to_rename):
    print(f"\t {f} -> {new_names[i]}")


if click.confirm(f"Do you want to rename these files?"):
    for i, f in enumerate(files_to_rename):
        fs.mv(f, new_names[i], recursive=True)

    print()
    print("Files successfully renamed!")

sys.exit(1)
