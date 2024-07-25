import requests
import ssl
from urllib3 import poolmanager

cookies = {'__dlauth_id': '4bf81b3dc374ebd63c4526d3399535debbac4a5d4983268f769c6d09028f2eb934fc0ba743a08d509a4d8f86ee98a343a5202d8b'}

url = 'https://iridl.ldeo.columbia.edu/SOURCES/.ECMWF/.S2S/.ECMF/.forecast/.perturbed/.sfc_precip/.tp/M/%281%29VALUES/S/%2825%20May%202017%29VALUES/Y/25.5/1.5/48/GRID/X/-123/1.5/-67.5/GRID/L/14/shiftdata/SOURCES/.ECMWF/.S2S/.ECMF/.forecast/.perturbed/.sfc_precip/.tp/M/%281%29VALUES/S/%2825%20May%202017%29VALUES/Y/25.5/1.5/48/GRID/X/-123/1.5/-67.5/GRID/sub/L_lag/removeGRID/data.nc'

class TLSAdapter(requests.adapters.HTTPAdapter):

    def init_poolmanager(self, connections, maxsize, block=False):
        """Create and initialize the urllib3 PoolManager."""
        ctx = ssl.create_default_context()
        ctx.set_ciphers('DEFAULT@SECLEVEL=1')
        self.poolmanager = poolmanager.PoolManager(
                num_pools=connections,
                maxsize=maxsize,
                block=block,
                ssl_version=ssl.PROTOCOL_TLS,
                ssl_context=ctx)

session = requests.session()
session.mount('https://', TLSAdapter())

r = session.get(url, timeout=30, cookies=cookies)
print(r.content)
