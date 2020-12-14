import os
import requests
import json
from typing import List

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
FILE_HISTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "history.json")

class RepositoryMirror():
  """Sync resources with Github respository."""

  def __init__(self, owner: str, repository: str,
               destination: str, files: List[str] = [],
               source_tree: bool = True):
    """Initialize repository mirror.

      Args:
        owner: repository owner
        repository: repository name
        destination: local (root) directory for repository contents
        files: default list of files in repository to sync
        source_tree: indicates if repository file structure should be maintained
    """
    self.owner = owner
    self.repository = repository
    self.destination = destination
    self.files = files
    self.source_tree = source_tree
    self.history = self._load_history()

  def _load_history(self):
    """Load history for local versions of files."""
    if not os.path.exists(FILE_HISTORY):
      return {}
    with open(FILE_HISTORY) as f:
      history = f.read()
    return json.loads(history)

  def _download_github_file(self, path: str):
    """Downloads a file from the Github repository.

      Args:
        path: path to the target file within the repository
    """
    # Reference https://docs.github.com/en/free-pro-team@latest/rest/overview/resources-in-the-rest-api
    # in case of API changes
    url = 'https://api.github.com/repos/{0}/{1}/contents/{2}'.format(self.owner, self.repository, path)
    # Get Github token from environment variable 'GITHUBTOKEN'
    # Using Github API v3 (raw output)
    headers = {
      'Authorization': 'token {}'.format(os.environ.get('GITHUBTOKEN', '')),
      'Accept': 'application/vnd.github.v3.raw'
    }

    # Check history for etag of the local version
    etag = self.history.get(url)
    if etag:
      # Header for conditional download if file is modified
      headers['If-None-Match'] = etag

    # Download file
    r = requests.get(url, headers=headers, stream=True)
    
    if r.status_code == 200:
      print("Status 200: Downloading...")
      # Create parent directory
      repo_parent = path.split("/")
      filename = repo_parent.pop()
      local_parent = self.destination

      if repo_parent and self.source_tree:
        # Add repository parents to maintain tree structure
        local_parent = os.path.join(local_parent, *repo_parent)

      os.makedirs(local_parent, exist_ok=True)
      file = os.path.join(local_parent, filename)

      with open(file, 'wb') as f:
        for chunk in r.iter_content():
            f.write(chunk)

      # Update history with new etag
      etag = r.headers.get('ETag')
      self.history[url] = etag
      with open(FILE_HISTORY, 'w') as f:
        f.write(json.dumps(self.history))

    elif r.status_code == 304:
      print("Status 304: File {0} not modified".format(url))
    else:
      print("Status {0}: error for file {1}".format(r.status_code, url))

  def sync(self, input_files: List[str] = []):
    """Update repository resources.

      Args:
        input_files: list of files to sync
    """
    # Use default list if no input
    if not input_files:
      input_files = self.files

    for file in input_files:
      self._download_github_file(file)


def get_censoredplanet_mirror():
  """Factory function to get mirror for Censored Planet repository."""
  owner = 'censoredplanet'
  repo = 'censoredplanet-scheduler'
  destination = os.path.join(PROJECT_ROOT, 'pipeline/assets/')
  files = [
    'signatures/false_positive_signatures.json', 
    'signatures/blockpage_signatures.json'
  ]
  return RepositoryMirror(owner, repo, destination,
                          files=files, source_tree=False)


if __name__ == "__main__":
  get_censoredplanet_mirror().sync()
