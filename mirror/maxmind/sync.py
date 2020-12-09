import os.path
import re
import requests
import shutil
import sys
import tarfile
import tempfile
import json

def update_maxmind(database_id: str, license: str, directory: str):
  """Update the Maxmind database to the most recent version.
  
  Args:
    database_id: database to download (GeoLite2-City or GeoLite2-ASN)
    license_key: license key for Maxmind account
    directory: path where Maxmind databases are located
  """
  url = 'https://download.maxmind.com/app/geoip_download?edition_id={0}&license_key={1}&suffix=tar.gz'.format(database_id, license)
  print('Downloading maxmind {0} ...'.format(database_id))

  # Make head request to quickly check date of latest db
  r = requests.head(url)
  print('HEAD Request for {0}: Status {1}'.format(database_id, r.status_code))
  if r.status_code != 200:
    print('Could not download', file=sys.stderr)
    return

  # Extract filename from headers
  # Content-Disposition format 'attachment; filename=GeoLite2-ASN_YYYYMMDD.tar.gz'
  contents = r.headers['Content-Disposition'] 
  contents = contents.split("filename=")[1]
  if contents.endswith(".tar.gz"):
    contents = contents[:-7]

  # Check filename and date
  regex = '^{0}_[0-9]'.format(database_id) + '{8}$'
  if not contents or not re.match(regex, contents):
    print('Unknown structure:', contents, file=sys.stderr)
    return
  date = contents[len(database_id) + 1:]
  print('File from', date)

  dest_path = os.path.join(directory, '{0}-{1}.mmdb'.format(database_id, date))
  if os.path.exists(dest_path):
    print('Already have latest version')
    return

  # Make get request to download db
  r = requests.get(url, stream=True)
  print('GET Request for {0}: Status {1}'.format(database_id, r.status_code))
  if r.status_code != 200:
    print('Could not download', file=sys.stderr)
    return

  # Download to temporary file
  tmp = tempfile.TemporaryFile()
  r.raw.decode_content = True
  shutil.copyfileobj(r.raw, tmp)
  tmp.seek(0)

  # Extract database
  with tarfile.open(mode='r:gz', fileobj=tmp) as tar:
    dest_path2 = os.path.join(directory, '{0}.mmdb'.format(database_id))
    print('Extracting...')
    src = tar.extractfile('{0}_{1}/{0}.mmdb'.format(database_id, date))
    dest = open(dest_path, 'wb')
    dest2 = open(dest_path2,'wb')
    shutil.copyfileobj(src, dest2)
    print('Done')


if __name__ == "__main__":
  project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
  directory = os.path.join(project_root, 'pipeline/assets/maxmind')
  config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "geoip.conf")

  # Check for config file in same directory as sync.py
  if os.path.exists(config_path):
    with open(config_path) as f:
      config = f.read()
    license = json.loads(config)["license_key"]
    update_maxmind('GeoLite2-City', license, directory)
    update_maxmind('GeoLite2-ASN', license, directory)
  else:
    print("Could not find GeoIP config file! Save file to {0}".format(config_path))

