import os
from google.cloud import secretmanager
from pathlib import Path

def cdsapi(func=None):

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
            cdsapirc1 = "url: https://cds.climate.copernicus.eu/api/v2"
            cdsapirc2 = f"key: {uid}:{api_key}"

            f = open(Path.home() / '.cdsapirc', mode='w+')
            f.write(cdsapirc1)
            f.write('\n')
            f.write(cdsapirc2)
            f.close()

    def wrapper(*args, **kwargs):
        get_secret()

        # call the function and return the result
        return func(*args, **kwargs)

    if not func:
        get_secret()
        return
    else:
        return wrapper
