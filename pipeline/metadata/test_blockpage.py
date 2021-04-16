"""Test Blockpage file parsing and lookup."""

import unittest

from pipeline.metadata import blockpage


class BlockpageTest(unittest.TestCase):

  def test_etc(self) -> None:
    matcher = blockpage.BlockpageMatcher()

    self.assertFalse(matcher.match_page("Thank you for using nginx."))
    self.assertTrue(matcher.match_page("fortinet.net"))
    self.assertIsNone(matcher.match_page("Not a blockpage or false positive"))
