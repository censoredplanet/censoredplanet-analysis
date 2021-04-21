"""Test Blockpage file parsing and lookup."""

import time
import unittest

from pipeline.metadata import blockpage


class BlockpageTest(unittest.TestCase):
  """Tests for the blockpage matcher."""

  def test_simple_blockpage_matches(self) -> None:
    matcher = blockpage.BlockpageMatcher()

    self.assertFalse(matcher.match_page("Thank you for using nginx."))
    self.assertTrue(matcher.match_page("fortinet.net"))
    self.assertIsNone(matcher.match_page("Not a blockpage or false positive"))

  def test_iran_blockpage_match(self) -> None:
    matcher = blockpage.BlockpageMatcher()
    page = '<html><head><meta http-equiv="Content-Type" content="text/html; charset=windows-1256"><title>MNN3-1(1)</title></head><body><iframe src="http://10.10.34.35:80" style="width: 100%; height: 100%" scrolling="no" marginwidth="0" marginheight="0" frameborder="0" vspace="0" hspace="0"></iframe></body></html>\r\n\r\n'
    self.assertTrue(matcher.match_page(page))

  def test_permission_false_positive_match(self) -> None:
    matcher = blockpage.BlockpageMatcher()
    page = '<HTML><HEAD>\n<TITLE>Access Denied</TITLE>\n</HEAD><BODY>\n<H1>Access Denied</H1>\n \nYou don\'t have permission to access "discover.com" on this server.<P>\nReference 18b535dd581604694259a71c660\n</BODY>\n</HTML>\n'
    self.assertFalse(matcher.match_page(page))

  def test_long_blockpage_performance(self) -> None:
    """Performance test for the blockpage matcher.

    Adding pathologically slow regexes to the false_positive_signatures.json
    or blockpage_signatures.json files can cause a performance hit that makes
    the overall pipeline to fail to complete. This test is designed to catch
    those regexes in case they're added in the future.
    """
    page = open("pipeline/metadata/test_files/long_blockpage.html").read()
    matcher = blockpage.BlockpageMatcher()

    start = time.perf_counter()
    for _ in range(100):
      matcher.match_page(page)
    end = time.perf_counter()

    self.assertLess(end - start, 10)

  def test_long_blockpage(self) -> None:
    # This blockpage is a random long page take from the data.
    # It is ~65k, which is near the truncated limit.
    page = open("pipeline/metadata/test_files/long_blockpage.html").read()
    matcher = blockpage.BlockpageMatcher()
    # Page classification should be None
    # to ensure that it exercises all regexes in the performance test
    self.assertIsNone(matcher.match_page(page))
