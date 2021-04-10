import json
import os
import re
from typing import Optional, Dict

import apache_beam.io.filesystems as apache_filesystems

# Signature filenames
FALSE_POSITIVES = 'false_positive_signatures.json'
BLOCKPAGES = 'blockpage_signatures.json'


class BlockpageMatcher:
  """Matcher to confirm blockpages or false positives."""

  def __init__(self, signature_folder: str) -> None:
    """Create a Blockpage Matcher.
    Args:
      signature_folder: a folder containing signature files.
        Either a gcs filepath or a local system folder.
    """
    false_positive_path = os.path.join(signature_folder, FALSE_POSITIVES)
    blockpage_path = os.path.join(signature_folder, BLOCKPAGES)

    self.false_positives = self._load_signatures(false_positive_path)
    self.blockpages = self._load_signatures(blockpage_path)

  def _load_signatures(self, path: str) -> Dict[str, re.Pattern]:
    """Load signatures for blockpage matching.

    Args:
      path: path to json file containing signatures

    Returns:
      Dictionary mapping fingerprints to signature patterns
    """
    signatures = {}
    with apache_filesystems.FileSystems.open(path) as f:
      for line in f:
        try:
          signature = json.loads(line.strip())
          # Patterns stored in BigQuery syntax,
          # so % represents any number of characters
          pattern = signature['pattern']
          # Convert to Python regex
          pattern = re.escape(pattern)
          pattern = pattern.replace('%', '.*')
          signatures[signature["fingerprint"]] = re.compile(pattern, re.DOTALL)
        except (json.decoder.JSONDecodeError, KeyError) as e:
          pass
    return signatures

  def match_page(self, page: str) -> Optional[bool]:
    """Check if the input page matches a known blockpage or false positive.

    Args:
      page: a string containing the HTTP body of the potential blockpage

    Returns:
      False if page matches a false positive signature.
      True if page matches a blockpage signature.
      None otherwise.
    """

    # Check false positives
    for fingerprint, pattern in self.false_positives.items():
      if pattern.search(page):
        return False

    # Check blockpages
    for fingerprint, pattern in self.blockpages.items():
      if pattern.search(page):
        return True

    # No signature match
    return None
