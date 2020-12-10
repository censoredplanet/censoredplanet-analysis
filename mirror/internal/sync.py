import os
import requests


def download_github_file(owner: str, repo: str, path: str, destination:str):
  """Downloads a file from a Github repository.
    
    Args:
      owner: repository owner
      repo: repository name
      path: path to the target file within the repository
      destination: directory to save file
  """
  # Reference https://docs.github.com/en/free-pro-team@latest/rest/overview/resources-in-the-rest-api
  # in case of API changes
  url = 'https://api.github.com/repos/{0}/{1}/contents/{2}'.format(owner, repo, path)
  # Get Github token from environment variable 'GITHUBTOKEN'
  # Using Github API v3 (raw output)
  headers = {
    'Authorization': 'token {}'.format(os.environ['GITHUBTOKEN']),
    'Accept': 'application/vnd.github.v3.raw'
  }

  # Download file
  r = requests.get(url, headers=headers, stream=True)
  if r.status_code == 200:
    # Create destination directory
    os.makedirs(destination, exist_ok=True)
    file = os.path.join(destination, path.split('/')[-1])
    with open(file, 'wb') as f:
      for chunk in r.iter_content():
          f.write(chunk)
  else:
    print("Status {0}: error for file {1}".format(r.status_code, url))


def sync():
  """Update Censored Planet resources."""
  owner = 'censoredplanet'
  repo = 'censoredplanet-scheduler'
  files = [
    'signatures/false_positive_signatures.json', 
    'signatures/blockpage_signatures.json'
  ]

  for file in files:
    destination = 'pipeline/assets/'
    download_github_file(owner, repo, file, destination)


if __name__ == "__main__":
  sync()
