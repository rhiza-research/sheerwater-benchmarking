"""Data download secret manager functions."""
import os
from pathlib import Path

from google.cloud import secretmanager
import salientsdk as sk


def postgres_write_password():
    """Get a postgres write password."""
    client = secretmanager.SecretManagerServiceClient()

    response = client.access_secret_version(
        request={"name": "projects/750045969992/secrets/sheerwater-postgres-write-password/versions/latest"})
    key = response.payload.data.decode("UTF-8")

    return key

def huggingface_read_token():
    """Get a postgres read password."""
    client = secretmanager.SecretManagerServiceClient()

    response = client.access_secret_version(
        request={"name": "projects/750045969992/secrets/hugging-face-read/versions/latest"})
    key = response.payload.data.decode("UTF-8")

    return key



def postgres_read_password():
    """Get a postgres read password."""
    client = secretmanager.SecretManagerServiceClient()

    response = client.access_secret_version(
        request={"name": "projects/750045969992/secrets/sheerwater-postgres-read-password/versions/latest"})
    key = response.payload.data.decode("UTF-8")

    return key


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


def general_secret(secret_name):
    """Get a secrete in our general format username:password from the secret manager."""
    # Check to see if the Salient secret exists
    path = Path.home() / f'.{secret_name}rc'
    if not os.path.exists(path):
        # Fetch the api key from google-secret-manager
        # Access the secret version.
        client = secretmanager.SecretManagerServiceClient()

        response = client.access_secret_version(
            request={"name": f"projects/750045969992/secrets/{secret_name}/versions/latest"})
        val = response.payload.data.decode("UTF-8")
        username, password = val.split(":")

        # # Write it to a file
        f = open(path, mode='w+')
        f.write(val)
        f.close()

        return username, password

    with open(path, mode='r') as f:
        lines = [line.strip() for line in f.readlines()]
    username, password = lines[0].split(":")
    return username, password


def tahmo_secret():
    """Get the TAHMO API login from the secret manager."""
    return general_secret("tahmo-api")


def gap_secret():
    """Get the GAP API login from the secret manager."""
    return general_secret("gap-api")


def salient_secret():
    """Get the Salient API login from the secret manager."""
    return general_secret("salient-api")


def salient_auth(func):
    """Decorator to run function with Salient API login permissions."""
    def wrapper(*args, **kwargs):
        # See if there are extra function args to run this remotely
        username, password = salient_secret()
        sk.login(username, password)
        sk.set_file_destination("./temp")

        return func(*args, **kwargs)
    return wrapper
