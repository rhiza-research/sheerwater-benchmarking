# Sheerwater Benchmarking

This repository holds all the analysis and visualization infrastructure for Sheerwater's benchmarking
project. The goal of the project is to benchmark ML- and physics-based weather and climate forecast regionally
around the globe, with a specific focus on model performance on the African continent.


## Benchmarking

Benchmarking code is run using python. Python packages and versions are managed by Rye. Install
Rye to get started.

### Install Rye

```
curl -sSf https://rye.astral.sh/get | bash
```

### Install non-python dependencies

```
brew install hdf5 netcdf
```

### Install python dependencies

```
cd sheerwater-benchmarking
rye sync
```

To add new python dependencies, run
```
rye add --sync <PACKAGE>
```
and push the updated project files. 

### Configure Google Cloud (optional, for cloud-based workflows) 
Install the Google Cloud CLI following instructions [here](https://cloud.google.com/sdk/docs/install). 

To set your default credentials for other applictions, like the gcloud Python API, run the following command and login to set up Application Default Credentials (ADC): 
```
gcloud auth application-default login
```

### Getting Started
To run python code in our python environment, use `rye run python <script.py> <args>`. To start a Jupyter notebook using an approriate kernel, run `rye run jupyter lab`. Our environment makes use of the `jupytext` package to run python files in a notebook environment. You can open any python script as a notebook by right clicking on the file within the jupyter lab environment and selecting `Open with' and `Notebook`. 



