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

## Deployment and Infrastructure

This repository is integrated with the Rhiza infrastructure for deployments:

### Pull Request Ephemeral Environments

Pull requests labeled with `PR-env` will automatically trigger ephemeral Grafana deployments via ArgoCD ApplicationSets. This allows testing of dashboard changes and configurations before merging.

To enable ephemeral deployment for your PR:
```bash
# Add the PR-env label using GitHub CLI
gh pr edit <PR_NUMBER> --add-label "PR-env"

# Or add the label via GitHub web interface
```

The ephemeral Grafana environment will be accessible at:
- `https://dev.shared.rhizaresearch.org/sheerwater-benchmarking/<PR_NUMBER>`

### Infrastructure Components

The `infrastructure/` directory contains:
- **terraform-config/** - Terraform configurations for Grafana instances (organizations, datasources, dashboards, etc.)
- **terraform-database/** - Database configuration module (imported by the main infrastructure repo)

### Database Configuration

Database access and configuration is managed centrally through the infrastructure repository. The `infrastructure/terraform-database/` module defines:
- PostgreSQL users and roles for Grafana
- Database permissions and grants
- Shared database access between production and ephemeral instances

This module is imported and executed by the infrastructure repository's `terraform/modules/rhiza-shared/database_config.tf`.

### ArgoCD Integration

This repository is monitored by ArgoCD ApplicationSets configured in the [rhiza-research/infrastructure](https://github.com/rhiza-research/infrastructure) repository for PR environments. Pull requests with the `PR-env` label trigger ephemeral environment creation/destruction. 



