"""Test Blockpage file parsing and lookup."""

import time
import unittest

from pipeline.metadata import blockpage

LONG_PAGE_FILE = "pipeline/metadata/test_files/long_blockpage.html"


class BlockpageTest(unittest.TestCase):
  """Tests for the blockpage matcher."""

  def test_signature_ordering(self) -> None:
    "Check first/last signatures in the blockpage dicts match file order"
    matcher = blockpage.BlockpageMatcher()

    blockpage_signatures = list(matcher.blockpages.keys())
    self.assertEqual("a_prod_barracuda_1", blockpage_signatures[0])
    self.assertEqual("f_gen_website_blocked_satellite",
                     blockpage_signatures[-1])

    false_positive_signatures = list(matcher.false_positives.keys())
    self.assertEqual("x_fpru_amazon", false_positive_signatures[0])
    self.assertEqual("satellite_fp_nginx_amazon", false_positive_signatures[-1])

  def test_simple_blockpage_matches(self) -> None:
    matcher = blockpage.BlockpageMatcher()

    self.assertFalse(matcher.match_page("Thank you for using nginx.")[0])
    self.assertTrue(matcher.match_page("fortinet.net")[0])
    self.assertIsNone(
        matcher.match_page("Not a blockpage or false positive")[0])

  def test_iran_blockpage(self) -> None:
    matcher = blockpage.BlockpageMatcher()
    page = '<html><head><meta http-equiv="Content-Type" content="text/html; charset=windows-1256"><title>MNN3-1(1)</title></head><body><iframe src="http://10.10.34.35:80" style="width: 100%; height: 100%" scrolling="no" marginwidth="0" marginheight="0" frameborder="0" vspace="0" hspace="0"></iframe></body></html>\r\n\r\n'
    match, signature = matcher.match_page(page)
    self.assertTrue(match)
    self.assertEqual(signature, 'b_nat_ir_national_1')

  def test_permission_false_positive(self) -> None:
    matcher = blockpage.BlockpageMatcher()
    page = '<HTML><HEAD>\n<TITLE>Access Denied</TITLE>\n</HEAD><BODY>\n<H1>Access Denied</H1>\n \nYou don\'t have permission to access "discover.com" on this server.<P>\nReference 18b535dd581604694259a71c660\n</BODY>\n</HTML>\n'
    match, signature = matcher.match_page(page)
    self.assertFalse(match)
    self.assertEqual(signature, 'x_on_this_server')

  def test_no_match(self) -> None:
    matcher = blockpage.BlockpageMatcher()
    page = '\u003cHTML\u003e\u003cHEAD\u003e\u003cTITLE\u003eA Website\u003c/TITLE\u003e\u003c/HEAD\u003e\u003cBODY\u003e\nThis site is not blocked!\u003cp\u003e\n\u003c/BODY\u003e\u003c/HTML\u003e\n'
    match, signature = matcher.match_page(page)
    self.assertIsNone(match)
    self.assertIsNone(signature)

  def test_generic_akamai_page(self) -> None:
    matcher = blockpage.BlockpageMatcher()
    page = '\u003cHTML\u003e\u003cHEAD\u003e\u003cTITLE\u003eError\u003c/TITLE\u003e\u003c/HEAD\u003e\u003cBODY\u003e\nAn error occurred while processing your request.\u003cp\u003e\nReference\u0026#32;\u0026#35;218\u0026#46;7714d23c\u0026#46;1568241844\u0026#46;522b6\n\u003c/BODY\u003e\u003c/HTML\u003e\n'
    match, signature = matcher.match_page(page)
    self.assertFalse(match)
    self.assertEqual(signature, 'x_generic_akamai_error')

  def test_spidergate(self) -> None:
    matcher = blockpage.BlockpageMatcher()
    page = 'URL blocked by SpIDer Gate'
    match, signature = matcher.match_page(page)
    self.assertTrue(match)
    self.assertEqual(signature, 'a_prod_drweb_3')

  def test_iterika(self) -> None:
    matcher = blockpage.BlockpageMatcher()
    page = 'HTTP/1.1 302 Moved Temporarily\r\nLocation: iterika.ru/blocked.html?UrlRedir=http%3A%2F%2Fblackberry.com%2f\r\nContent-Length: 0\r\nCache-Control: max-age=0, no-cache, no-store, must-revalidate\r\nPragma: no-cache\r\nConnection: close\r\n\r\n'
    match, signature = matcher.match_page(page)
    self.assertTrue(match)
    self.assertEqual(signature, 'c_isp_ru_iterika')

  def test_divo(self) -> None:
    matcher = blockpage.BlockpageMatcher()
    page = 'HTTP/1.1 302 Moved Temporarily\r\nLocation: https://www.divo.ru/%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D0%B9-%D1%80%D0%B5%D1%81%D1%83%D1%80%D1%81-%D0%B7%D0%B0%D0%B1%D0%BB%D0%BE%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD/?UrlRedir=http%3A%2F%2Fwww.hizb-ut-tahrir.org%2f\r\nContent-Length: 0\r\nCache-Control: max-age=0, no-cache, no-store, must-revalidate\r\nPragma: no-cache\r\nConnection: close\r\n\r\n'
    match, signature = matcher.match_page(page)
    self.assertTrue(match)
    self.assertEqual(signature, 'c_isp_ru_divo_2')

  def test_special_characters(self) -> None:
    matcher = blockpage.BlockpageMatcher()
    # yapf: disable
    page = '''\u003chtml\u003e\r\n\u003chead\u003e\r\n\u003ctitle\u003eæ¡æç¶²é é»æ\u003c/title\u003e\r\n\u003cmeta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\"\u003e\r\n\u003cMETA HTTP-EQUIV=\"PRAGMA\" CONTENT=\"NO-CACHE\"\u003e\r\n\u003cmeta name=\"viewport\" content=\"initial-scale=1.0\"\u003e\r\n\u003cstyle\u003e\r\n  #content {\r\n    border:3px solid#aaa;\r\n    background-color:#fff;\r\n    margin:1.5em;\r\n    padding:1.5em;\r\n    font-family:Tahoma,Helvetica,Arial,sans-serif;\r\n    font-size:1em;\r\n  }\r\n  h1 {\r\n    font-size:1.3em;\r\n    font-weight:bold;\r\n    color:#196390;\r\n  }\r\n  b {\r\n    font-weight:normal;\r\n    color:#196390;\r\n  }\r\n\u003c/style\u003e\r\n\u003c/head\u003e\r\n\u003cbody bgcolor=\"#e7e8e9\"\u003e\r\n\u003cdiv id=\"content\"\u003e\r\n\u003ch1\u003eéç¾è³è¨å®å¨é²è­· - æ¡æç¶²é é»æ\u003c/h1\u003e\r\n\u003cp\u003eå¦ææ¨çå°è©²ç«é¢è³è¨ï¼è¡¨ç¤ºæ¨è¢«å¤æ·å­åéæ­£å¸¸è¡çºç¶²ç« \u003cspan style=\"color:red;\"\u003e(æ¡æç¶²ç«)\u003c/span\u003e\u003c/p\u003e\r\n\u003cp\u003eè³è¨èª²å·²å°æ­¤ç¶²é é»æï¼å¦æç¢ºå®è©²ç¶²é æ¯è¢«èª¤å¤è«è¯ç¹«: éç¾è³è¨èª²-ç³»çµ±ç¶­éçµï¼è¬è¬ã\u003c/p\u003e\r\n\u003cp\u003e\u003cb\u003eä½¿ç¨è:\u003c/b\u003e 141.212.123.175 \u003c/p\u003e\r\n\u003cp\u003e\u003cb\u003eç¶²å:\u003c/b\u003e rtyutgyhefdafioasfjhjhi.com/ \u003c/p\u003e\r\n\u003cp\u003e\u003cb\u003eåé¡:\u003c/b\u003e command-and-control \u003c/p\u003e\r\n\u003c/div\u003e\r\n\u003c/body\u003e\r\n\u003c/html\u003e\r\n'''
    # yapf: enable
    match, signature = matcher.match_page(page)
    self.assertTrue(match)
    self.assertEqual(signature, 'e_unk_style_red')

  def test_ordered_signatures(self) -> None:
    """Test page with multiple matching signatures returns the first."""
    matcher = blockpage.BlockpageMatcher()

    page = '''<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
              <html>
              <head>
              <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
                  <title>Application Control Violation</title>
                  <style type="text/css">
                      html, body { margin: 0; padding: 0; font-family: Verdana, Arial, sans-serif; font-size: 10pt; }
                      h1, h2 { height: 82px; text-indent: -999em; margin: 0; padding: 0; margin: 0; }
                      div { margin: 0; padding: 0; }
                      div.header { background: url(http://url.fortinet.net:8008/XX/YY/ZZ/CI/MGPGHGPGPFGHCDPFGGOGFGEH) 0 0 repeat-x; height: 82px; }
                      div.header h1 { background: url(http://url.fortinet.net:8008/XX/YY/ZZ/CI/MGPGHGPGPFGHCDPFGGHGFHBGCHEGPFBGAHAH) 0 0 no-repeat; }
                      div.header h2 { background: url(http://url.fortinet.net:8008/XX/YY/ZZ/CI/MGPGHGPGPFGHCDPFGGOGFGEH) 0 -82px no-repeat; width: 160px; float: right; }
                      div.sidebar { width: 195px; height: 200px; float: left; }
                      div.main { padding: 5px; margin-left: 195px; }
                      div.buttons { margin-top: 30px; text-align: right; }
                      div.app-title { background:url(http://www.fortiguard.com/app_logos/large36774.png) no-repeat; margin: 8px 0px; height: 32px; text-indent: 36px; line-height: 20px; font-size: 17px; padding-top:5px; }
                      div.app-info { padding-bottom: 5px; text-indent: 18px; }
                      h3 { margin: 36px 0; font-size: 16pt; }
                      .blocked      h3 { color: #c00; }
                      h2.fgd_icon { background: url(http://url.fortinet.net:8008/XX/YY/ZZ/CI/MGPGHGPGPFGHCDPFGGOGFGEH) 0 -166px repeat-x; width: 90px; height: 92px; margin: 48px auto; }
                      .blocked      h2.fgd_icon { background-position: 0 -166px; }
                      form { width: 300px; margin: 30px 0; }
                      label { display: block; width: 300px; margin: 5px 0; line-height: 25px; }
                      label input { width: 200px; border: 1px solid #7f9db9; height: 20px; float: right; }
                  </style>
              </head>
              <body class="blocked">
                  <div class="header">
                      <h2>Powered By Fortinet</h2>
                      <h1>FortiGate Application Control</h1>
                  </div>
                  <div class="sidebar">
                      <h2 class="fgd_icon">blocked</h2>
                  </div>
                  <div class="main">
              <h3>Application Blocked!</h3>
              <div class="notice">You have attempted to use an application which is in violation of your internet usage policy.</div>
              <div class="app-title">Hola.Unblocker</div>
              <div class="app-info">Category: Proxy</div>
              <div class="app-info">URL: http://hola.org/</div>
              <div class="app-info">User name: </div>
              <div class="app-info">Group name: </div>
              <div class="app-info">Policy: cc3e8682-9551-51ea-65c7-758f3c094ffc</div>    </div>
              </body>
              </html>'''

    match, signature = matcher.match_page(page)
    self.assertTrue(match)
    self.assertEqual(signature, 'a_prod_fortinet_2')
    # This page matches multiple a_prod_fortinet_* signatures.
    # This test makes sure we return the match for the earliest one.

  def test_unicode(self) -> None:
    """Test to check unicode string matching in Chinese."""
    matcher = blockpage.BlockpageMatcher()
    page = '''<html>
              <head>
                <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
                <title>无法访问</title>
              </head>
              <body style="text-align: center;margin: 0 auto;width: 100%;">
              <div style="margin: 0 auto;width: 600px;text-align: left;">
                <div style="background: #4074e1;margin-top: 100px; color: #FFF;padding: 20px;">
                  <span>温馨提示</span>
                </div>
              <div style="border: 1px solid #DDD;padding: 20px;background: #edf1ff;">
              <h4 style="text-align: center;">该网站暂时无法进行访问</h4>
              <p>因该网站未进行备案或涉及发布违禁信息，违反工信部、公安部相关规定而无法访问。</p>
              </br>
              <p>如需恢复网站正常访问，网站管理员需及时进行相应处理：</p>
              <br>
              <p>1、要完成网站的ICP备案，请到UCloud备案系统进行操作。</p>
              <p>2、若涉及到公安备案，请登录全国公安机关互联网站安全管理服务平台
                <a href="http://www.beian.gov.cn/">www.beian.gov.cn</a> 进行操作。
              </p>
              <br>
              <p>如有相关疑问，管理员可咨询UCloud备案客服。</p>
              <br>
              <p>感谢您的配合！</p>
              <a href="https://www.ucloud.cn/">
              <img src="https://www.ucloud.cn/static/style/images/index/2017/nav-logo.png"  alt="UCloud云计算" align="right"  />
              </a>
              <br>
              </div>
            </div>
            </body>
            </html>'''
    match, signature = matcher.match_page(page)
    self.assertTrue(match)
    self.assertEqual(signature, 'd_corp_cn_ucloud')

  def test_long_blockpage_performance(self) -> None:
    """Performance test for the blockpage matcher.

    Adding pathologically slow regexes to the false_positive_signatures.json
    or blockpage_signatures.json files can cause a performance hit that causes
    the overall pipeline to fail to complete. This test is designed to catch
    those regexes in case they're added in the future.
    """
    with open(LONG_PAGE_FILE, encoding='utf-8') as page:
      matcher = blockpage.BlockpageMatcher()
      content = page.read()

      start = time.perf_counter()
      for _ in range(100):
        matcher.match_page(content)
      end = time.perf_counter()

      self.assertLess(end - start, 10)

  def test_long_blockpage(self) -> None:
    # This blockpage is a random long page take from the data.
    # It is ~65k, which is near the truncated limit.
    with open(LONG_PAGE_FILE, encoding='utf-8') as page:
      matcher = blockpage.BlockpageMatcher()
      # Page classification should be None
      # to ensure that it exercises all regexes in the performance test
      self.assertIsNone(matcher.match_page(page.read())[0])
