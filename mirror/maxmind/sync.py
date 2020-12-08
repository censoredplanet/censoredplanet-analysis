import os.path
import re
import requests
import shutil
import sys
import tarfile
import tempfile


def update_maxmind(database_id, license, directory):
  # Download to temporary file
  tmp = tempfile.TemporaryFile()

  url = 'https://download.maxmind.com/app/geoip_download?edition_id={0}&license_key={1}&suffix=tar.gz'.format(database_id, license)
  print('Downloading maxmind {0} ...'.format(database_id))
  r = requests.get(url, stream=True)
  print('Status', r.status_code)
  if r.status_code != 200:
    print('Could not download', file=sys.stderr)
    return

  r.raw.decode_content = True
  shutil.copyfileobj(r.raw, tmp)

  tmp.seek(0)
  with tarfile.open(mode='r:gz', fileobj=tmp) as tar:

    contents = tar.getnames()
    regex = '^{0}_[0-9]'.format(database_id) + '{8}$'
    if not contents or not re.match(regex, contents[0]):
      print('Unknown structure:', contents, file=sys.stderr)
      return

    # Figure out what date the database is from
    date = contents[0][len(database_id) + 1:]
    print('File from', date)

    dest_path = os.path.join(directory, '{0}-{1}.mmdb'.format(database_id, date))
    if os.path.exists(dest_path):
      print('Already have latest version')
      return
      
    # Extract database
    dest_path2 = os.path.join(directory, '{0}.mmdb'.format(database_id))
    print('Extracting...')
    src = tar.extractfile('{0}_{1}/{0}.mmdb'.format(database_id, date))
    dest = open(dest_path, 'wb')
    dest2 = open(dest_path2,'wb')
    shutil.copyfileobj(src, dest2)
    print('Done')

