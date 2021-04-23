"""Matcher for response pages to blockpage signatures."""

import json
import io
import pkgutil
import re
from typing import Optional, Dict

# Signature filenames
FALSE_POSITIVES = 'data/false_positive_signatures.json'
BLOCKPAGES = 'data/blockpage_signatures.json'


def _load_signatures(filepath: str) -> Dict[str, re.Pattern]:
  """Load signatures for blockpage matching.

  Args:
    filepath: relative path to json file containing signatures

  Returns:
    Dictionary mapping fingerprints to signature patterns
  """
  data = pkgutil.get_data(__name__, filepath)
  if not data:
    raise FileNotFoundError(f"Couldn't find file {filepath}")
  content = io.TextIOWrapper(io.BytesIO(data), encoding='utf-8')

  signatures = {}
  for line in content.readlines():
    if line != '\n':
      signature = json.loads(line.strip())
      pattern = signature['pattern']
      fingerprint = signature['fingerprint']

      signatures[fingerprint] = re.compile(pattern, re.DOTALL)
  return signatures


class BlockpageMatcher:
  """Matcher to confirm blockpages or false positives."""

  def __init__(self) -> None:
    """Create a Blockpage Matcher."""
    self.false_positives = _load_signatures(FALSE_POSITIVES)
    self.blockpages = _load_signatures(BLOCKPAGES)

  def match_page(self, page: str) -> Optional[bool]:
    """Check if the input page matches a known blockpage or false positive.

    Args:
      page: a string containing the HTTP body of the potential blockpage

    Returns:
      False if page matches a false positive signature.
      True if page matches a blockpage signature.
      None otherwise.
    """
    for fingerprint, pattern in self.false_positives.items():
      if pattern.search(page):
        return False

    for fingerprint, pattern in self.blockpages.items():
      if pattern.search(page):
        return True

    # No signature match
    return None
