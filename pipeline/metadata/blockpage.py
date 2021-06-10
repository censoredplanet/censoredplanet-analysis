"""Matcher for response pages to blockpage signatures."""

from collections import OrderedDict
import json
import io
import pkgutil
import re
from typing import Optional, Dict, Tuple

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

  signatures = OrderedDict()
  for line in content.readlines():
    if line != '\n':
      signature = json.loads(line.strip())
      pattern = signature['pattern']
      fingerprint = signature['fingerprint']

      signatures[fingerprint] = re.compile(pattern, re.DOTALL)
  return signatures


class BlockpageMatcher:
  """Matcher to confirm blockpages or false positives."""

  # TODO update this class to input signatures for easier testability.

  def __init__(self) -> None:
    """Create a Blockpage Matcher."""
    self.false_positives = _load_signatures(FALSE_POSITIVES)
    self.blockpages = _load_signatures(BLOCKPAGES)

  def match_page(self, page: str) -> Tuple[Optional[bool], Optional[str]]:
    """Check if the input page matches a known blockpage or false positive.

    Args:
      page: a string containing the HTTP body of the potential blockpage

    Returns:
      (match_outcome, match_fingerprint)
      match_outcome is
        True if page matches a blockpage signature.
        False if page matches a false positive signature.
        None otherwise.
      match_fingerprint is a signature for a blockpage/fp like 'a_prod_cisco'
    """
    for fingerprint, pattern in self.false_positives.items():
      if pattern.search(page):
        return (False, fingerprint)

    for fingerprint, pattern in self.blockpages.items():
      if pattern.search(page):
        return (True, fingerprint)

    return (None, None)
