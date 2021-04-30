"""Test Blockpage file parsing and lookup."""

import time
import unittest

from pipeline.metadata import blockpage


class BlockpageTest(unittest.TestCase):
  """Tests for the blockpage matcher."""

  def test_simple_blockpage_matches(self) -> None:
    matcher = blockpage.BlockpageMatcher()

    self.assertFalse(matcher.match_page("Thank you for using nginx.")[0])
    self.assertTrue(matcher.match_page("fortinet.net")[0])
    self.assertIsNone(
        matcher.match_page("Not a blockpage or false positive")[0])

  def test_iran_blockpage_match(self) -> None:
    matcher = blockpage.BlockpageMatcher()
    page = '<html><head><meta http-equiv="Content-Type" content="text/html; charset=windows-1256"><title>MNN3-1(1)</title></head><body><iframe src="http://10.10.34.35:80" style="width: 100%; height: 100%" scrolling="no" marginwidth="0" marginheight="0" frameborder="0" vspace="0" hspace="0"></iframe></body></html>\r\n\r\n'
    match, signature = matcher.match_page(page)
    self.assertTrue(match)
    self.assertEqual(signature, 'b_nat_ir_national_1')

  def test_permission_false_positive_match(self) -> None:
    matcher = blockpage.BlockpageMatcher()
    page = '<HTML><HEAD>\n<TITLE>Access Denied</TITLE>\n</HEAD><BODY>\n<H1>Access Denied</H1>\n \nYou don\'t have permission to access "discover.com" on this server.<P>\nReference 18b535dd581604694259a71c660\n</BODY>\n</HTML>\n'
    match, signature = matcher.match_page(page)
    self.assertFalse(match)
    self.assertEqual(signature, 'x_on_this_server')

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
    self.assertIsNone(matcher.match_page(page)[0])

  def test_complex_blockpage_matching(self) -> None:
    """Test blockpage matching for data with detected anomalies."""
    matcher = blockpage.BlockpageMatcher()

    # yapf: disable
    pages = [
      '\u003cHTML\u003e\u003cHEAD\u003e\u003cTITLE\u003eError\u003c/TITLE\u003e\u003c/HEAD\u003e\u003cBODY\u003e\nAn error occurred while processing your request.\u003cp\u003e\nReference\u0026#32;\u0026#35;218\u0026#46;7714d23c\u0026#46;1568241844\u0026#46;522b6\n\u003c/BODY\u003e\u003c/HTML\u003e\n',
      'URL blocked by SpIDer Gate',
      '\u003cHTML\u003e\u003cHEAD\u003e\u003cTITLE\u003eA Website\u003c/TITLE\u003e\u003c/HEAD\u003e\u003cBODY\u003e\nThis site is not blocked!\u003cp\u003e\n\u003c/BODY\u003e\u003c/HTML\u003e\n',
      'HTTP/1.1 302 Moved Temporarily\r\nLocation: iterika.ru/blocked.html?UrlRedir=http%3A%2F%2Fblackberry.com%2f\r\nContent-Length: 0\r\nCache-Control: max-age=0, no-cache, no-store, must-revalidate\r\nPragma: no-cache\r\nConnection: close\r\n\r\n',
      'HTTP/1.1 302 Moved Temporarily\r\nLocation: https://www.divo.ru/%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D0%B9-%D1%80%D0%B5%D1%81%D1%83%D1%80%D1%81-%D0%B7%D0%B0%D0%B1%D0%BB%D0%BE%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD/?UrlRedir=http%3A%2F%2Fwww.hizb-ut-tahrir.org%2f\r\nContent-Length: 0\r\nCache-Control: max-age=0, no-cache, no-store, must-revalidate\r\nPragma: no-cache\r\nConnection: close\r\n\r\n',
      '''\u003chtml\u003e\r\n\u003chead\u003e\r\n\u003ctitle\u003eæ¡æç¶²é é»æ\u003c/title\u003e\r\n\u003cmeta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\"\u003e\r\n\u003cMETA HTTP-EQUIV=\"PRAGMA\" CONTENT=\"NO-CACHE\"\u003e\r\n\u003cmeta name=\"viewport\" content=\"initial-scale=1.0\"\u003e\r\n\u003cstyle\u003e\r\n  #content {\r\n    border:3px solid#aaa;\r\n    background-color:#fff;\r\n    margin:1.5em;\r\n    padding:1.5em;\r\n    font-family:Tahoma,Helvetica,Arial,sans-serif;\r\n    font-size:1em;\r\n  }\r\n  h1 {\r\n    font-size:1.3em;\r\n    font-weight:bold;\r\n    color:#196390;\r\n  }\r\n  b {\r\n    font-weight:normal;\r\n    color:#196390;\r\n  }\r\n\u003c/style\u003e\r\n\u003c/head\u003e\r\n\u003cbody bgcolor=\"#e7e8e9\"\u003e\r\n\u003cdiv id=\"content\"\u003e\r\n\u003ch1\u003eéç¾è³è¨å®å¨é²è­· - æ¡æç¶²é é»æ\u003c/h1\u003e\r\n\u003cp\u003eå¦ææ¨çå°è©²ç«é¢è³è¨ï¼è¡¨ç¤ºæ¨è¢«å¤æ·å­åéæ­£å¸¸è¡çºç¶²ç« \u003cspan style=\"color:red;\"\u003e(æ¡æç¶²ç«)\u003c/span\u003e\u003c/p\u003e\r\n\u003cp\u003eè³è¨èª²å·²å°æ­¤ç¶²é é»æï¼å¦æç¢ºå®è©²ç¶²é æ¯è¢«èª¤å¤è«è¯ç¹«: éç¾è³è¨èª²-ç³»çµ±ç¶­éçµï¼è¬è¬ã\u003c/p\u003e\r\n\u003cp\u003e\u003cb\u003eä½¿ç¨è:\u003c/b\u003e 141.212.123.175 \u003c/p\u003e\r\n\u003cp\u003e\u003cb\u003eç¶²å:\u003c/b\u003e rtyutgyhefdafioasfjhjhi.com/ \u003c/p\u003e\r\n\u003cp\u003e\u003cb\u003eåé¡:\u003c/b\u003e command-and-control \u003c/p\u003e\r\n\u003c/div\u003e\r\n\u003c/body\u003e\r\n\u003c/html\u003e\r\n'''
    ]
    # yapf: enable

    expected_matches = [
        False,
        True,
        None,
        True,
        True,
        True,
    ]
    expected_signatures = [
        'x_generic_akamai_error', 'a_prod_drweb_3', None, 'c_isp_ru_iterika',
        'c_isp_ru_divo_2', 'e_unk_style_red'
    ]

    match_pairs = [matcher.match_page(page) for page in pages]
    matches, signatures = zip(*match_pairs)
    self.assertListEqual(list(matches), expected_matches)
    self.assertListEqual(list(signatures), expected_signatures)
