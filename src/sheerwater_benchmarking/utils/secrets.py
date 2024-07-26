import os
from google.cloud import secretmanager
from pathlib import Path

def cdsapi_secret(func=None):

    def get_secret():
        # Check to see if the CDS secret exists
        if not os.path.exists("~/.cdsapirc"):
            #Fetch the api key from google-secret-manager
            # Access the secret version.
            client = secretmanager.SecretManagerServiceClient()

            response = client.access_secret_version(request={"name": "projects/750045969992/secrets/cdsapi-uid/versions/latest"})
            uid = response.payload.data.decode("UTF-8")

            response = client.access_secret_version(request={"name": "projects/750045969992/secrets/cdsapi-key/versions/latest"})
            api_key = response.payload.data.decode("UTF-8")

            #Write it to a file
            url = "https://cds.climate.copernicus.eu/api/v2"
            key = f"{uid}:{api_key}"
            cdsapirc1 = "url: {url}"
            cdsapirc2 = f"key: {key}"

            f = open(Path.home() / '.cdsapirc', mode='w+')
            f.write(cdsapirc1)
            f.write('\n')
            f.write(cdsapirc2)
            f.close()
            return url, key

    def wrapper(*args, **kwargs):
        get_secret()

        # call the function and return the result
        return func(*args, **kwargs)

    if not func:
        return get_secret()
    else:
        return wrapper
