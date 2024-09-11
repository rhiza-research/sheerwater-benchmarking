"""Data download secret manager functions."""
import os
from google.cloud import secretmanager
from pathlib import Path


def cdsapi_secret():
    """Fetches the CDS API secret from the secret manager."""
    # Check to see if the CDS secret exists
    path = Path.home() / ".cdsapirc"
    if not os.path.exists(path):
        # Fetch the api key from google-secret-manager
        # Access the secret version.
        client = secretmanager.SecretManagerServiceClient()

        response = client.access_secret_version(
            request={"name": "projects/750045969992/secrets/cdsapi-beta-geneveive/versions/latest"})
        key = response.payload.data.decode("UTF-8")

        # Write it to a file
        url = "https://cds-beta.climate.copernicus.eu/api"
        cdsapirc1 = f"url: {url}"
        cdsapirc2 = f"key: {key}"

        f = open(path, mode='w+')
        f.write(cdsapirc1)
        f.write('\n')
        f.write(cdsapirc2)
        f.close()
        return url, key

    with open(path, mode='r') as f:
        lines = [line.strip() for line in f.readlines()[:2]]
    url, key = [line.split(":", 1)[1].strip() for line in lines]
    return url, key


def ecmwf_secret():
    """Get the ECMWF API key from the secret manager."""
    # Check to see if the ECMWF secret exists
    path = Path.home() / '.ecmwfrc'
    if not os.path.exists(path):
        # Fetch the api key from google-secret-manager
        # Access the secret version.
        client = secretmanager.SecretManagerServiceClient()

        response = client.access_secret_version(
            request={"name": "projects/750045969992/secrets/ecmwf-iri/versions/latest"})
        val = response.payload.data.decode("UTF-8")
        secret = f"key: {val}"

        # Write it to a file
        f = open(path, mode='w+')
        f.write(secret)
        f.write('\n')
        f.close()

        return val

    with open(path, mode='r') as f:
        lines = [line.strip() for line in f.readlines()]
    val = [line.split(":", 1)[1].strip() for line in lines][0]
    return val
