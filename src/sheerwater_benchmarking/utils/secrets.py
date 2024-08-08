import os
from google.cloud import secretmanager
from pathlib import Path
import json


def cdsapi_secret():
    """Fetches the CDS API secret from the secret manager."""
    # Check to see if the CDS secret exists
    if not os.path.exists(Path.home() / ".cdsapirc"):
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

        f = open(Path.home() / '.cdsapirc', mode='w+')
        f.write(cdsapirc1)
        f.write('\n')
        f.write(cdsapirc2)
        f.close()
        return url, key
    else:
        with open(Path.home() / '.cdsapirc', mode='r') as f:
            lines = [line.strip() for line in f.readlines()[:2]]
        url, key = [l.split(":", 1)[1].strip() for l in lines]
        return url, key


def ecmwf_secret():
    # Check to see if the ECMWF secret exists
    if not os.path.exists("~/.ecmwfrc"):
        # Fetch the api key from google-secret-manager
        # Access the secret version.
        client = secretmanager.SecretManagerServiceClient()

        response = client.access_secret_version(
            request={"name": "projects/750045969992/secrets/ecmwf-iri/versions/latest"})
        uid = response.payload.data.decode("UTF-8")

        # Write it to a file
        f = open(Path.home() / '.ecmwfrc', mode='w+')
        json.dump(uid, f)
        f.close()

    f = open(Path.home() / '.ecmwfrc', mode='r+')
    val = json.load(f)
    return json.loads(val)["ecmwf_key"]
