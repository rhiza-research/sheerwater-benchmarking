
import glob
import gcsfs

files = glob.glob('imerg/*.nc4')
fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')

for f in files:
    date = f.split('.')[4].split('-')[0]

    dest = 'gs://sheerwater-datalake/imerg/' + date + '.nc'

    print(f + '->' + dest)
    fs.put_file(f, dest)
