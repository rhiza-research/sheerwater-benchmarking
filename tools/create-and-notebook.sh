"""
Create the SheerWater coiled software environment (include conda binaries, e.g., xesmf)
and then start a notebook in that environment .
"""
python ./tools/create-coiled-software-environment.py \
  && coiled notebook start --software sheerwater-env --name $USER "$@"
