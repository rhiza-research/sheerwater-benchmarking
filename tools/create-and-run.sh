"""
Create the SheerWater coiled software environment (include conda binaries, e.g., xesmf)
and then uses coiled run to run comment with useful configuration .
"""
python ./tools/create-coiled-software-environment.py \
  && coiled run --software sheerwater-env --keepalive 20m --name $USER "$@"
