"""
Interpets the arguments passed to tools.sheerwater_benchmarking.coils as conda
depndencies in the pyproject.toml and instanties the SheerWater coiled software
environment with them.
"""

import coiled
import tomllib

# Read pip dependencies from requirements.lock
pip_dependencies = []
with open("requirements.lock") as r:
    pip_dependencies = [x.strip() for x in r.readlines()
                        if x.strip() != "" and x.strip()[0] != '#']

# Get coiled dependencies from the tools section
conda_dependencies = []
with open("pyproject.toml", "rb") as f:
    data = tomllib.load(f)

    try:
        conda_dependencies = data['tool']['sheerwater_benchmarking']['coiled']['conda-dependencies']
    except KeyError:
        pass

# Create software environment
coiled.create_software_environment(
    name="sheerwater-env",
    conda={
        "channels": ["conda-forge"],
        "dependencies": conda_dependencies,
    },
    pip="requirements.lock",
    include_local_code=True,
)
