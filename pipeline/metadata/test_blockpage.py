"""Test Blockpage file parsing and lookup."""

import unittest

from pipeline.metadata import blockpage


class BlockpageTest(unittest.TestCase):

  def test_etc(self) -> None:
    signature_folder = "pipeline/metadata/test_files"
    matcher = blockpage.BlockpageMatcher(signature_folder)

    self.assertFalse(matcher.match_page("Thank you for using nginx."))
    self.assertTrue(matcher.match_page("fortinet.net"))
    self.assertIsNone(matcher.match_page("Not a blockpage or false positive"))
