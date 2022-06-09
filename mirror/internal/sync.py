"""Sync local copies of Censored Planet resources."""

import json
import os
from typing import Any, Dict, List, Optional

import requests

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
FILE_HISTORY = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "history.json")


class RepositoryMirror():
  """Sync resources with Github respository."""

  def __init__(  # pylint: disable=dangerous-default-value
      self,
      owner: Optional[str],
      repository: str,
      destination: str,
      files: List[str] = [],
      source_tree: bool = True,
      github: bool = False) -> None:
    """Initialize repository mirror.

      Args:
        owner: repository owner
        repository: repository name
        destination: local (root) directory for repository contents
        files: default list of files in repository to sync
        source_tree: indicates if repository file structure should be maintained
        github: indicates if repository is hosted on Github
    """
    self.owner = owner
    self.repository = repository
    self.destination = destination
    self.files = files
    self.source_tree = source_tree
    self.github = github
    self.history = self._load_history()

  def _load_history(self) -> Dict[str, Any]:
    """Load history for local versions of files."""
    if not os.path.exists(FILE_HISTORY):
      return {}
    with open(FILE_HISTORY, encoding='utf-8') as f:
      history = f.read()
    return json.loads(history)

  def _download_file(self, path: str) -> None:
    """Downloads a file from the repository.

      Args:
        path: path to the target file within the repository
    """
    if self.github:
      # Reference https://docs.github.com/en/free-pro-team@latest/rest/overview/resources-in-the-rest-api
      # in case of API changes
      url = f'https://api.github.com/repos/{self.owner}/{self.repository}/contents/{path}'
      # Get Github token from environment variable 'GITHUBTOKEN'
      # Using Github API v3 (raw output)
      token = os.environ.get('GITHUBTOKEN', '')
      headers = {
          'Authorization': f'token {token}',
          'Accept': 'application/vnd.github.v3.raw'
      }
    else:
      url = self.repository + path
      headers = {}

    # Check history for etag of the local version
    etag = self.history.get(url)
    if etag:
      # Header for conditional download if file is modified
      headers['If-None-Match'] = etag

    # Download file
    req = requests.get(url, headers=headers, stream=True)

    if req.status_code == 200:
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

      with open(file, 'wb') as file1:
        for chunk in req.iter_content():
          file1.write(chunk)

      # Update history with new etag
      etag = req.headers.get('ETag')
      self.history[url] = etag
      with open(FILE_HISTORY, 'w', encoding='utf-8') as file2:
        file2.write(json.dumps(self.history))

    elif req.status_code == 304:
      print(f"Status 304: File {url} not modified")
    else:
      print("Status {req.status_code}: error for file {url}")

  def sync(self, input_files: List[str] = []) -> None:  # pylint: disable=dangerous-default-value
    """Update repository resources.

      Args:
        input_files: list of files to sync
    """
    # Use default list if no input
    if not input_files:
      input_files = self.files

    for file in input_files:
      self._download_file(file)


def get_censoredplanet_mirror() -> RepositoryMirror:
  """Factory function to get mirror for Censored Planet repository."""
  repo = 'https://assets.censoredplanet.org'
  destination = os.path.join(PROJECT_ROOT, 'pipeline/assets/')
  files = ['/false_positive_signatures.json', '/blockpage_signatures.json']
  return RepositoryMirror(
      None, repo, destination, files=files, source_tree=False)


if __name__ == "__main__":
  get_censoredplanet_mirror().sync()
