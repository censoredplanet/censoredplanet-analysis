"""Unit tests for hyperquack."""

import json
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam.testing.util as beam_test_util

from pipeline.metadata import hyperquack
from pipeline.metadata.schema import HyperquackRow



class HyperquackTest(unittest.TestCase):

  def test_process_hyperquack_lines(self) -> None:
    data_filenames = [
        "CP_Satellite-2020-09-02-12-00-01/interference.json",
    ]

    _data = [
        {
            "vp": "104.17.67.3",
            "location": {},
            "service": "https",
            "test_url": "1337x.to",
            "response": [
                {
                "matches_template": False,
                "response": {
                    "status_line": "200 OK",
                    "headers": {
                    "Alt-Svc": [
                        "h3=\":443\"; ma=86400, h3-29=\":443\"; ma=86400"
                    ],
                    "Cf-Cache-Status": [
                        "DYNAMIC"
                    ],
                    "Cf-Ray": [
                        "6f4e4da53e5e7e37-DTW"
                    ],
                    "Content-Type": [
                        "text/html"
                    ],
                    "Date": [
                        "Fri, 01 Apr 2022 03:40:26 GMT"
                    ],
                    "Expect-Ct": [
                        "max-age=604800, report-uri=\"https://report-uri.cloudflare.com/cdn-cgi/beacon/expect-ct\""
                    ],
                    "Nel": [
                        "{\"success_fraction\":0,\"report_to\":\"cf-nel\",\"max_age\":604800}"
                    ],
                    "Report-To": [
                        "{\"endpoints\":[{\"url\":\"https:\\/\\/a.nel.cloudflare.com\\/report\\/v3?s=CeekyUcFAsvbkFlepHbKEW%2FDmCnCjE292yXAu%2FS124I%2B1kOoXk2ooUbMVNILm4v2qZzty%2F%2FLyeXwKbJ09BaM%2BpJbOIrThX3LjWlf%2B%2BEalpcqEDxseLNzj%2B6AjQ%3D%3D\"}],\"group\":\"cf-nel\",\"max_age\":604800}"
                    ],
                    "Server": [
                        "cloudflare"
                    ],
                    "Vary": [
                        "Accept-Encoding"
                    ],
                    "X-Frame-Options": [
                        "DENY"
                    ]
                    },
                    "body": "<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"utf-8\">\n<meta http-equiv=\"X-UA-Compatible\" content=\"IE=edge\">\n<title>Torrent Search Engine | 1337x.to</title>\n<meta name=\"description\" content=\"1337x is a search engine to find your favorite torrents.\">\n<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n<link rel=\"stylesheet\" href=\"/css/jquery-ui.css\">\n<link rel=\"stylesheet\" href=\"/css/icons.css\">\n<link rel=\"stylesheet\" href=\"/css/scrollbar.css\">\n<link rel=\"stylesheet\" href=\"/css/style.css?ver=2.5\">\n<link rel=\"shortcut icon\" href=\"/favicon.ico\">\n<!--[if lt IE 9]><script src = \"/js/html5shiv.js\"></script><![endif]-->\n<style id=\"antiClickjack\">body{display:none !important;}</style>\n<script type=\"text/javascript\" id=\"antiClickjackJS\">\r\nif (self === top) {\r\nvar antiClickjack = document.getElementById(\"antiClickjack\");\r\nantiClickjack.parentNode.removeChild(antiClickjack);\r\n} else {\r\ntop.location = self.location;\r\n}\r\n</script>\n<script async src='/cdn-cgi/challenge-platform/h/b/scripts/invisible.js?ts=1648782000'></script></head>\n<body class=\"search-index-page\">\n<div class=\"mobile-menu\"></div>\n<div class=\"top-bar\">\n<div class=\"container\">\n<div class=\"top-bar-left\">\n</div>\n<ul class=\"top-bar-nav\">\n<li><a href=\"/register\">Register</a></li>\n<li class=\"active\"><a href=\"/login\">Login</a></li>\n</ul>\n</div>\n</div>\n<header>\n<div class=\"container\">\n<a href=\"#\" class=\"navbar-menu\"><span></span><span></span><span></span></a>\n<nav>\n<ul class=\"main-navigation\">\n<li class=\"green\"><a href=\"/home/\" title=\"Full Home Page\">Full Home Page</a></li>\n<li><a href=\"/top-100\" title=\"Top 100 Torrents\">Top 100</a></li>\n<li><a href=\"/trending\" title=\"Trending Torrents\">Trending</a></li>\n<li><a href=\"/contact\" title=\"Contact\">Contact</a></li>\n<li><a href=\"/upload\" title=\"Upload Torrent File\">Upload</a></li>\n</ul>\n</nav>\n</div>\n</header>\n<main class=\"container\">\n<div class=\"row\">\n<div class=\"col-8 col-push-2 page-content \">\n<div class=\"logo\"><a href=\"/home/\"><img alt=\"logo\" src=\"/images/logo.svg\"></a></div>\n<div class=\"search-box\">\n<form id=\"search-index-form\" method=\"get\" action=\"/srch\">\n<input type=\"search\" placeholder=\"Search for torrents..\" id=\"autocomplete\" name=\"search\" class=\"ui-autocomplete-input form-control\">\n<button type=\"submit\" class=\"btn btn-search\"><i class=\"flaticon-search\"></i><span>Search</span></button>\n</form>\n</div>\n<div class=\"news\">\n<div class=\"box-info\">\n<div class=\"box-info-heading clearfix\"><h1> 1337x Domains</h1> <span class=\"box-info-right box-info-time\"><i class=\"flaticon-time\"></i>2 years ago</span></div>\n<div class=\"box-info-detail clearfix\">\n<p>1337x newest alternative domain is <a href=\"https://1337x.gd\">1337x.gd</a>, for full list of alternative domains see <a href=\"/about\">about</a> page.</p>\n</div>\n</div>\n</div>\n</div>\n</div>\n</main>\n<ul class=\"search-categories\">\n<li>\n<h3><a href=\"/popular-movies\" title=\"Movies\"><span class=\"icon\"><i class=\"flaticon-movies\"></i></span>Movies </a></h3>\n<a href=\"/movie-library/1/\" class=\"library\">Library</a>\n</li>\n<li>\n<h3><a href=\"/popular-tv\" title=\"Television\"><span class=\"icon\"><i class=\"flaticon-tv\"></i></span>Television </a></h3>\n<a href=\"/series-library/a/1/\" class=\"library\">Library</a>\n</li>\n<li>\n<h3><a href=\"/popular-games\" title=\"Games\"><span class=\"icon\"><i class=\"flaticon-games\"></i></span>Games </a></h3>\n</li>\n<li>\n<h3><a href=\"/popular-music\" title=\"Musics\"><span class=\"icon\"><i class=\"flaticon-music\"></i></span>Music </a></h3>\n</li>\n <li>\n<h3><a href=\"/popular-apps\" title=\"Applications\"><span class=\"icon\"><i class=\"flaticon-apps\"></i></span>Applications </a></h3>\n</li>\n<li>\n<h3><a href=\"/popular-anime\" title=\"Other\"><span class=\"icon\"><i class=\"flaticon-ninja-portrait\"></i></span>Anime </a></h3>\n</li>\n<li>\n<h3><a href=\"/popular-documentaries\" title=\"Documentries\"><span class=\"icon\"><i class=\"flaticon-documentary\"></i></span>Documentaries </a></h3>\n</li>\n<li>\n<h3><a href=\"/popular-other\" title=\"Other\"><span class=\"icon\"><i class=\"flaticon-other\"></i></span>Other </a></h3>\n</li>\n<li>\n<h3><a href=\"/popular-xxx\" title=\"XXX\"><span class=\"icon\"><i class=\"flaticon-xxx\"></i></span>XXX </a></h3>\n</li>\n</ul>\n<footer>\n<div class=\"bitcoin\">\n<div class=\"bitcoin-icon-wrap\">\n<span class=\"bitcoin-icon\"><i class=\"flaticon-bitcoin red\"></i></span>\n</div>\n<span class=\"bitcoin-text\"><span>Bitcoin Donate: </span><a href=\"bitcoin:3Q1337xL2i6jXrXqZ5aMfhN4wp366GQc44\">3Q1337xL2i6jXrXqZ5aMfhN4wp366GQc44</a></span>\n</div>\n<a class=\"scroll-top\" href=\"#\"><i class=\"flaticon-up\"></i></a>\n<ul>\n<li><a href=\"/\">Home</a></li>\n<li class=\"active\"><a href=\"/home/\">Full Home Page</a></li>\n<li><a href=\"/contact\">Dmca</a></li>\n<li><a href=\"/contact\">Contact</a></li>\n</ul>\n<p class=\"info\">1337x 2007 - 2022</p>\n</footer>\n<script src=\"/js/jquery-1.11.0.min.js\"></script>\n<script src=\"/js/jquery-ui.js\"></script>\n<script src=\"/js/auto-searchv2.js\"></script>\n<script src=\"/js/main.js\"></script>\n<script>\r\nif (window.top !== window.self) window.top.location.replace(window.self.location.href);\r\n</script>\n<script type=\"text/javascript\">(function(){window['__CF$cv$params']={r:'6f4e4da53e5e7e37',m:'IsBBcj6rc3JGvAlajuHf0auy14zkPGvn53Ywxiu6tzc-1648784426-0-AauJI5DJvSeq+v7p0q5sGHqmEPaJfJHSLStTvHQY4MTqSl5uyykrkkpRXmi8PGMK2p2bAlt1k7uxkV7Lc52SzOkwJN1NCAhcIIx4MPtsbVcSbdtWA7DCM2gddJfuRDtaLA==',s:[0xc3ee4548ae,0x139b8c3c2a],u:'/cdn-cgi/challenge-platform/h/b'}})();</script></body>\n</html>",
                    "TlsVersion": 771,
                    "CipherSuite": 49195,
                    "Certificate": "MIIFLjCCBNSgAwIBAgIQBhTy7YX2of9FwDxEyfQb1jAKBggqhkjOPQQDAjBKMQswCQYDVQQGEwJVUzEZMBcGA1UEChMQQ2xvdWRmbGFyZSwgSW5jLjEgMB4GA1UEAxMXQ2xvdWRmbGFyZSBJbmMgRUNDIENBLTMwHhcNMjEwNzA5MDAwMDAwWhcNMjIwNzA4MjM1OTU5WjB1MQswCQYDVQQGEwJVUzETMBEGA1UECBMKQ2FsaWZvcm5pYTEWMBQGA1UEBxMNU2FuIEZyYW5jaXNjbzEZMBcGA1UEChMQQ2xvdWRmbGFyZSwgSW5jLjEeMBwGA1UEAxMVc25pLmNsb3VkZmxhcmVzc2wuY29tMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEw2Ard/LZZh8zIyYzluZM+813stVbMghpUwR7cVIW1zkUzxpp6KFHXdXy11axUfv40aFFzbRtXwrJYQDZoJ3mR6OCA28wggNrMB8GA1UdIwQYMBaAFKXON+rrsHUOlGeItEX62SQQh5YfMB0GA1UdDgQWBBSBNxPW8hCKlK0p3egif26mcj5IeDA2BgNVHREELzAtgggxMzM3eC50b4IKKi4xMzM3eC50b4IVc25pLmNsb3VkZmxhcmVzc2wuY29tMA4GA1UdDwEB/wQEAwIHgDAdBgNVHSUEFjAUBggrBgEFBQcDAQYIKwYBBQUHAwIwewYDVR0fBHQwcjA3oDWgM4YxaHR0cDovL2NybDMuZGlnaWNlcnQuY29tL0Nsb3VkZmxhcmVJbmNFQ0NDQS0zLmNybDA3oDWgM4YxaHR0cDovL2NybDQuZGlnaWNlcnQuY29tL0Nsb3VkZmxhcmVJbmNFQ0NDQS0zLmNybDA+BgNVHSAENzA1MDMGBmeBDAECAjApMCcGCCsGAQUFBwIBFhtodHRwOi8vd3d3LmRpZ2ljZXJ0LmNvbS9DUFMwdgYIKwYBBQUHAQEEajBoMCQGCCsGAQUFBzABhhhodHRwOi8vb2NzcC5kaWdpY2VydC5jb20wQAYIKwYBBQUHMAKGNGh0dHA6Ly9jYWNlcnRzLmRpZ2ljZXJ0LmNvbS9DbG91ZGZsYXJlSW5jRUNDQ0EtMy5jcnQwDAYDVR0TAQH/BAIwADCCAX0GCisGAQQB1nkCBAIEggFtBIIBaQFnAHYAKXm+8J45OSHwVnOfY6V35b5XfZxgCvj5TV0mXCVdx4QAAAF6i3ixQwAABAMARzBFAiBVd/jDNSKeCa66mS/GeSTohSOfLkturC5wsXdDzbt3swIhAKDAClmoVXgjLDoNioYTpHPwUZIQgB/Plf1ec6T3PM1HAHYAQcjKsd8iRkoQxqE6CUKHXk4xixsD6+tLx2jwkGKWBvYAAAF6i3ixMwAABAMARzBFAiEApaMxT/bS7Ze1bOo2XiEP/UZwx+C/g+fFYFzIrxR5B+MCIHLweH3GxT3dedYmJfZ62Wh2sjtVAW63GMLwjx38X5ZbAHUA36Veq2iCTx9sre64X04+WurNohKkal6OOxLAIERcKnMAAAF6i3ixXQAABAMARjBEAiB3RpGRk+GsJds1lCcfu0DPoUqA6DFv2fkvDqmeXx2XZQIgCU61EV+lUuiQMoEyiyYdgP/GevUZHP6NwptXJwjkJx0wCgYIKoZIzj0EAwIDSAAwRQIgaWbSos6z7xucrvst6O9y18Th+beVBmQ1l5WwahHf4V8CIQDCMixPJrHUGenFeOHDGtxzI51YO50zl4ufbyP6n8zyWQ=="
                },
                "error": "Cipher suites do not match;Certificates do not match;Status lines do not match;Content-Length header field missing;Bodies do not match;",
                "start_time": "2022-03-31T23:40:25.75122271-04:00",
                "end_time": "2022-03-31T23:40:26.287401066-04:00"
                },
                {
                "matches_template": False,
                "response": {
                    "status_line": "200 OK",
                    "headers": {
                    "Alt-Svc": [
                        "h3=\":443\"; ma=86400, h3-29=\":443\"; ma=86400"
                    ],
                    "Cf-Cache-Status": [
                        "DYNAMIC"
                    ],
                    "Cf-Ray": [
                        "6f4e4dc7afa77e79-DTW"
                    ],
                    "Content-Type": [
                        "text/html"
                    ],
                    "Date": [
                        "Fri, 01 Apr 2022 03:40:31 GMT"
                    ],
                    "Expect-Ct": [
                        "max-age=604800, report-uri=\"https://report-uri.cloudflare.com/cdn-cgi/beacon/expect-ct\""
                    ],
                    "Nel": [
                        "{\"success_fraction\":0,\"report_to\":\"cf-nel\",\"max_age\":604800}"
                    ],
                    "Report-To": [
                        "{\"endpoints\":[{\"url\":\"https:\\/\\/a.nel.cloudflare.com\\/report\\/v3?s=Ga2zkkqNFnWQMWl0mcWGTfGqwRWCGDqSpBJyOuZffEaQyaDFQDKT6ne5g51rsPrIKK5AE0lNdeEJzz%2BFxXOV%2BEBegyL6%2B2qCZkR9woI8cckdLR4YoaYZ3XoQ%2Fw%3D%3D\"}],\"group\":\"cf-nel\",\"max_age\":604800}"
                    ],
                    "Server": [
                        "cloudflare"
                    ],
                    "Vary": [
                        "Accept-Encoding"
                    ],
                    "X-Frame-Options": [
                        "DENY"
                    ]
                    },
                    "body": "<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"utf-8\">\n<meta http-equiv=\"X-UA-Compatible\" content=\"IE=edge\">\n<title>Torrent Search Engine | 1337x.to</title>\n<meta name=\"description\" content=\"1337x is a search engine to find your favorite torrents.\">\n<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n<link rel=\"stylesheet\" href=\"/css/jquery-ui.css\">\n<link rel=\"stylesheet\" href=\"/css/icons.css\">\n<link rel=\"stylesheet\" href=\"/css/scrollbar.css\">\n<link rel=\"stylesheet\" href=\"/css/style.css?ver=2.5\">\n<link rel=\"shortcut icon\" href=\"/favicon.ico\">\n<!--[if lt IE 9]><script src = \"/js/html5shiv.js\"></script><![endif]-->\n<style id=\"antiClickjack\">body{display:none !important;}</style>\n<script type=\"text/javascript\" id=\"antiClickjackJS\">\r\nif (self === top) {\r\nvar antiClickjack = document.getElementById(\"antiClickjack\");\r\nantiClickjack.parentNode.removeChild(antiClickjack);\r\n} else {\r\ntop.location = self.location;\r\n}\r\n</script>\n<script async src='/cdn-cgi/challenge-platform/h/b/scripts/invisible.js?ts=1648782000'></script></head>\n<body class=\"search-index-page\">\n<div class=\"mobile-menu\"></div>\n<div class=\"top-bar\">\n<div class=\"container\">\n<div class=\"top-bar-left\">\n</div>\n<ul class=\"top-bar-nav\">\n<li><a href=\"/register\">Register</a></li>\n<li class=\"active\"><a href=\"/login\">Login</a></li>\n</ul>\n</div>\n</div>\n<header>\n<div class=\"container\">\n<a href=\"#\" class=\"navbar-menu\"><span></span><span></span><span></span></a>\n<nav>\n<ul class=\"main-navigation\">\n<li class=\"green\"><a href=\"/home/\" title=\"Full Home Page\">Full Home Page</a></li>\n<li><a href=\"/top-100\" title=\"Top 100 Torrents\">Top 100</a></li>\n<li><a href=\"/trending\" title=\"Trending Torrents\">Trending</a></li>\n<li><a href=\"/contact\" title=\"Contact\">Contact</a></li>\n<li><a href=\"/upload\" title=\"Upload Torrent File\">Upload</a></li>\n</ul>\n</nav>\n</div>\n</header>\n<main class=\"container\">\n<div class=\"row\">\n<div class=\"col-8 col-push-2 page-content \">\n<div class=\"logo\"><a href=\"/home/\"><img alt=\"logo\" src=\"/images/logo.svg\"></a></div>\n<div class=\"search-box\">\n<form id=\"search-index-form\" method=\"get\" action=\"/srch\">\n<input type=\"search\" placeholder=\"Search for torrents..\" id=\"autocomplete\" name=\"search\" class=\"ui-autocomplete-input form-control\">\n<button type=\"submit\" class=\"btn btn-search\"><i class=\"flaticon-search\"></i><span>Search</span></button>\n</form>\n</div>\n<div class=\"news\">\n<div class=\"box-info\">\n<div class=\"box-info-heading clearfix\"><h1> 1337x Domains</h1> <span class=\"box-info-right box-info-time\"><i class=\"flaticon-time\"></i>2 years ago</span></div>\n<div class=\"box-info-detail clearfix\">\n<p>1337x newest alternative domain is <a href=\"https://1337x.gd\">1337x.gd</a>, for full list of alternative domains see <a href=\"/about\">about</a> page.</p>\n</div>\n</div>\n</div>\n</div>\n</div>\n</main>\n<ul class=\"search-categories\">\n<li>\n<h3><a href=\"/popular-movies\" title=\"Movies\"><span class=\"icon\"><i class=\"flaticon-movies\"></i></span>Movies </a></h3>\n<a href=\"/movie-library/1/\" class=\"library\">Library</a>\n</li>\n<li>\n<h3><a href=\"/popular-tv\" title=\"Television\"><span class=\"icon\"><i class=\"flaticon-tv\"></i></span>Television </a></h3>\n<a href=\"/series-library/a/1/\" class=\"library\">Library</a>\n</li>\n<li>\n<h3><a href=\"/popular-games\" title=\"Games\"><span class=\"icon\"><i class=\"flaticon-games\"></i></span>Games </a></h3>\n</li>\n<li>\n<h3><a href=\"/popular-music\" title=\"Musics\"><span class=\"icon\"><i class=\"flaticon-music\"></i></span>Music </a></h3>\n</li>\n <li>\n<h3><a href=\"/popular-apps\" title=\"Applications\"><span class=\"icon\"><i class=\"flaticon-apps\"></i></span>Applications </a></h3>\n</li>\n<li>\n<h3><a href=\"/popular-anime\" title=\"Other\"><span class=\"icon\"><i class=\"flaticon-ninja-portrait\"></i></span>Anime </a></h3>\n</li>\n<li>\n<h3><a href=\"/popular-documentaries\" title=\"Documentries\"><span class=\"icon\"><i class=\"flaticon-documentary\"></i></span>Documentaries </a></h3>\n</li>\n<li>\n<h3><a href=\"/popular-other\" title=\"Other\"><span class=\"icon\"><i class=\"flaticon-other\"></i></span>Other </a></h3>\n</li>\n<li>\n<h3><a href=\"/popular-xxx\" title=\"XXX\"><span class=\"icon\"><i class=\"flaticon-xxx\"></i></span>XXX </a></h3>\n</li>\n</ul>\n<footer>\n<div class=\"bitcoin\">\n<div class=\"bitcoin-icon-wrap\">\n<span class=\"bitcoin-icon\"><i class=\"flaticon-bitcoin red\"></i></span>\n</div>\n<span class=\"bitcoin-text\"><span>Bitcoin Donate: </span><a href=\"bitcoin:3Q1337xL2i6jXrXqZ5aMfhN4wp366GQc44\">3Q1337xL2i6jXrXqZ5aMfhN4wp366GQc44</a></span>\n</div>\n<a class=\"scroll-top\" href=\"#\"><i class=\"flaticon-up\"></i></a>\n<ul>\n<li><a href=\"/\">Home</a></li>\n<li class=\"active\"><a href=\"/home/\">Full Home Page</a></li>\n<li><a href=\"/contact\">Dmca</a></li>\n<li><a href=\"/contact\">Contact</a></li>\n</ul>\n<p class=\"info\">1337x 2007 - 2022</p>\n</footer>\n<script src=\"/js/jquery-1.11.0.min.js\"></script>\n<script src=\"/js/jquery-ui.js\"></script>\n<script src=\"/js/auto-searchv2.js\"></script>\n<script src=\"/js/main.js\"></script>\n<script>\r\nif (window.top !== window.self) window.top.location.replace(window.self.location.href);\r\n</script>\n<script type=\"text/javascript\">(function(){window['__CF$cv$params']={r:'6f4e4dc7afa77e79',m:'GC3coBmgyukN.QAGRbl.1xQL.t0QcCe8Ljyd3o4PxEc-1648784431-0-AQ9XmE3qbq3GBr8iPzK0NGD2VlI/m7BAQy05oa3G2I4qWFD+5D03zyslNt1u6QpDpg6K9/O1gQYBePf90KwhTcRD9poI5vtD2Ed1aE6HSR2WKY5hoDp+LC/T8Y8LPusLHA==',s:[0x0d764350a9,0x0730074f2c],u:'/cdn-cgi/challenge-platform/h/b'}})();</script></body>\n</html>",
                    "TlsVersion": 771,
                    "CipherSuite": 49195,
                    "Certificate": "MIIFLjCCBNSgAwIBAgIQBhTy7YX2of9FwDxEyfQb1jAKBggqhkjOPQQDAjBKMQswCQYDVQQGEwJVUzEZMBcGA1UEChMQQ2xvdWRmbGFyZSwgSW5jLjEgMB4GA1UEAxMXQ2xvdWRmbGFyZSBJbmMgRUNDIENBLTMwHhcNMjEwNzA5MDAwMDAwWhcNMjIwNzA4MjM1OTU5WjB1MQswCQYDVQQGEwJVUzETMBEGA1UECBMKQ2FsaWZvcm5pYTEWMBQGA1UEBxMNU2FuIEZyYW5jaXNjbzEZMBcGA1UEChMQQ2xvdWRmbGFyZSwgSW5jLjEeMBwGA1UEAxMVc25pLmNsb3VkZmxhcmVzc2wuY29tMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEw2Ard/LZZh8zIyYzluZM+813stVbMghpUwR7cVIW1zkUzxpp6KFHXdXy11axUfv40aFFzbRtXwrJYQDZoJ3mR6OCA28wggNrMB8GA1UdIwQYMBaAFKXON+rrsHUOlGeItEX62SQQh5YfMB0GA1UdDgQWBBSBNxPW8hCKlK0p3egif26mcj5IeDA2BgNVHREELzAtgggxMzM3eC50b4IKKi4xMzM3eC50b4IVc25pLmNsb3VkZmxhcmVzc2wuY29tMA4GA1UdDwEB/wQEAwIHgDAdBgNVHSUEFjAUBggrBgEFBQcDAQYIKwYBBQUHAwIwewYDVR0fBHQwcjA3oDWgM4YxaHR0cDovL2NybDMuZGlnaWNlcnQuY29tL0Nsb3VkZmxhcmVJbmNFQ0NDQS0zLmNybDA3oDWgM4YxaHR0cDovL2NybDQuZGlnaWNlcnQuY29tL0Nsb3VkZmxhcmVJbmNFQ0NDQS0zLmNybDA+BgNVHSAENzA1MDMGBmeBDAECAjApMCcGCCsGAQUFBwIBFhtodHRwOi8vd3d3LmRpZ2ljZXJ0LmNvbS9DUFMwdgYIKwYBBQUHAQEEajBoMCQGCCsGAQUFBzABhhhodHRwOi8vb2NzcC5kaWdpY2VydC5jb20wQAYIKwYBBQUHMAKGNGh0dHA6Ly9jYWNlcnRzLmRpZ2ljZXJ0LmNvbS9DbG91ZGZsYXJlSW5jRUNDQ0EtMy5jcnQwDAYDVR0TAQH/BAIwADCCAX0GCisGAQQB1nkCBAIEggFtBIIBaQFnAHYAKXm+8J45OSHwVnOfY6V35b5XfZxgCvj5TV0mXCVdx4QAAAF6i3ixQwAABAMARzBFAiBVd/jDNSKeCa66mS/GeSTohSOfLkturC5wsXdDzbt3swIhAKDAClmoVXgjLDoNioYTpHPwUZIQgB/Plf1ec6T3PM1HAHYAQcjKsd8iRkoQxqE6CUKHXk4xixsD6+tLx2jwkGKWBvYAAAF6i3ixMwAABAMARzBFAiEApaMxT/bS7Ze1bOo2XiEP/UZwx+C/g+fFYFzIrxR5B+MCIHLweH3GxT3dedYmJfZ62Wh2sjtVAW63GMLwjx38X5ZbAHUA36Veq2iCTx9sre64X04+WurNohKkal6OOxLAIERcKnMAAAF6i3ixXQAABAMARjBEAiB3RpGRk+GsJds1lCcfu0DPoUqA6DFv2fkvDqmeXx2XZQIgCU61EV+lUuiQMoEyiyYdgP/GevUZHP6NwptXJwjkJx0wCgYIKoZIzj0EAwIDSAAwRQIgaWbSos6z7xucrvst6O9y18Th+beVBmQ1l5WwahHf4V8CIQDCMixPJrHUGenFeOHDGtxzI51YO50zl4ufbyP6n8zyWQ=="
                },
                "error": "Cipher suites do not match;Certificates do not match;Status lines do not match;Content-Length header field missing;Bodies do not match;",
                "start_time": "2022-03-31T23:40:31.289443406-04:00",
                "end_time": "2022-03-31T23:40:31.763808644-04:00"
                },
                {
                "matches_template": False,
                "response": {
                    "status_line": "200 OK",
                    "headers": {
                    "Alt-Svc": [
                        "h3=\":443\"; ma=86400, h3-29=\":443\"; ma=86400"
                    ],
                    "Cf-Cache-Status": [
                        "DYNAMIC"
                    ],
                    "Cf-Ray": [
                        "6f4e4df73ba4fe12-DTW"
                    ],
                    "Content-Type": [
                        "text/html"
                    ],
                    "Date": [
                        "Fri, 01 Apr 2022 03:40:39 GMT"
                    ],
                    "Expect-Ct": [
                        "max-age=604800, report-uri=\"https://report-uri.cloudflare.com/cdn-cgi/beacon/expect-ct\""
                    ],
                    "Nel": [
                        "{\"success_fraction\":0,\"report_to\":\"cf-nel\",\"max_age\":604800}"
                    ],
                    "Report-To": [
                        "{\"endpoints\":[{\"url\":\"https:\\/\\/a.nel.cloudflare.com\\/report\\/v3?s=CfFb%2BVtX28LZ82%2B3Zl3N7V8ZgL43MtzJ5%2B%2F9K2LX2uhha%2BcSHp%2BRKBpMfkCpfI%2Bjc%2B7WLw2ZtOK%2B%2FCwSTOJzRY1%2B337pNvKdVeQuv9rikAc%2FWamgBujwPf9yfw%3D%3D\"}],\"group\":\"cf-nel\",\"max_age\":604800}"
                    ],
                    "Server": [
                        "cloudflare"
                    ],
                    "Vary": [
                        "Accept-Encoding"
                    ],
                    "X-Frame-Options": [
                        "DENY"
                    ]
                    },
                    "body": "<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"utf-8\">\n<meta http-equiv=\"X-UA-Compatible\" content=\"IE=edge\">\n<title>Torrent Search Engine | 1337x.to</title>\n<meta name=\"description\" content=\"1337x is a search engine to find your favorite torrents.\">\n<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n<link rel=\"stylesheet\" href=\"/css/jquery-ui.css\">\n<link rel=\"stylesheet\" href=\"/css/icons.css\">\n<link rel=\"stylesheet\" href=\"/css/scrollbar.css\">\n<link rel=\"stylesheet\" href=\"/css/style.css?ver=2.5\">\n<link rel=\"shortcut icon\" href=\"/favicon.ico\">\n<!--[if lt IE 9]><script src = \"/js/html5shiv.js\"></script><![endif]-->\n<style id=\"antiClickjack\">body{display:none !important;}</style>\n<script type=\"text/javascript\" id=\"antiClickjackJS\">\r\nif (self === top) {\r\nvar antiClickjack = document.getElementById(\"antiClickjack\");\r\nantiClickjack.parentNode.removeChild(antiClickjack);\r\n} else {\r\ntop.location = self.location;\r\n}\r\n</script>\n<script async src='/cdn-cgi/challenge-platform/h/b/scripts/invisible.js?ts=1648782000'></script></head>\n<body class=\"search-index-page\">\n<div class=\"mobile-menu\"></div>\n<div class=\"top-bar\">\n<div class=\"container\">\n<div class=\"top-bar-left\">\n</div>\n<ul class=\"top-bar-nav\">\n<li><a href=\"/register\">Register</a></li>\n<li class=\"active\"><a href=\"/login\">Login</a></li>\n</ul>\n</div>\n</div>\n<header>\n<div class=\"container\">\n<a href=\"#\" class=\"navbar-menu\"><span></span><span></span><span></span></a>\n<nav>\n<ul class=\"main-navigation\">\n<li class=\"green\"><a href=\"/home/\" title=\"Full Home Page\">Full Home Page</a></li>\n<li><a href=\"/top-100\" title=\"Top 100 Torrents\">Top 100</a></li>\n<li><a href=\"/trending\" title=\"Trending Torrents\">Trending</a></li>\n<li><a href=\"/contact\" title=\"Contact\">Contact</a></li>\n<li><a href=\"/upload\" title=\"Upload Torrent File\">Upload</a></li>\n</ul>\n</nav>\n</div>\n</header>\n<main class=\"container\">\n<div class=\"row\">\n<div class=\"col-8 col-push-2 page-content \">\n<div class=\"logo\"><a href=\"/home/\"><img alt=\"logo\" src=\"/images/logo.svg\"></a></div>\n<div class=\"search-box\">\n<form id=\"search-index-form\" method=\"get\" action=\"/srch\">\n<input type=\"search\" placeholder=\"Search for torrents..\" id=\"autocomplete\" name=\"search\" class=\"ui-autocomplete-input form-control\">\n<button type=\"submit\" class=\"btn btn-search\"><i class=\"flaticon-search\"></i><span>Search</span></button>\n</form>\n</div>\n<div class=\"news\">\n<div class=\"box-info\">\n<div class=\"box-info-heading clearfix\"><h1> 1337x Domains</h1> <span class=\"box-info-right box-info-time\"><i class=\"flaticon-time\"></i>2 years ago</span></div>\n<div class=\"box-info-detail clearfix\">\n<p>1337x newest alternative domain is <a href=\"https://1337x.gd\">1337x.gd</a>, for full list of alternative domains see <a href=\"/about\">about</a> page.</p>\n</div>\n</div>\n</div>\n</div>\n</div>\n</main>\n<ul class=\"search-categories\">\n<li>\n<h3><a href=\"/popular-movies\" title=\"Movies\"><span class=\"icon\"><i class=\"flaticon-movies\"></i></span>Movies </a></h3>\n<a href=\"/movie-library/1/\" class=\"library\">Library</a>\n</li>\n<li>\n<h3><a href=\"/popular-tv\" title=\"Television\"><span class=\"icon\"><i class=\"flaticon-tv\"></i></span>Television </a></h3>\n<a href=\"/series-library/a/1/\" class=\"library\">Library</a>\n</li>\n<li>\n<h3><a href=\"/popular-games\" title=\"Games\"><span class=\"icon\"><i class=\"flaticon-games\"></i></span>Games </a></h3>\n</li>\n<li>\n<h3><a href=\"/popular-music\" title=\"Musics\"><span class=\"icon\"><i class=\"flaticon-music\"></i></span>Music </a></h3>\n</li>\n <li>\n<h3><a href=\"/popular-apps\" title=\"Applications\"><span class=\"icon\"><i class=\"flaticon-apps\"></i></span>Applications </a></h3>\n</li>\n<li>\n<h3><a href=\"/popular-anime\" title=\"Other\"><span class=\"icon\"><i class=\"flaticon-ninja-portrait\"></i></span>Anime </a></h3>\n</li>\n<li>\n<h3><a href=\"/popular-documentaries\" title=\"Documentries\"><span class=\"icon\"><i class=\"flaticon-documentary\"></i></span>Documentaries </a></h3>\n</li>\n<li>\n<h3><a href=\"/popular-other\" title=\"Other\"><span class=\"icon\"><i class=\"flaticon-other\"></i></span>Other </a></h3>\n</li>\n<li>\n<h3><a href=\"/popular-xxx\" title=\"XXX\"><span class=\"icon\"><i class=\"flaticon-xxx\"></i></span>XXX </a></h3>\n</li>\n</ul>\n<footer>\n<div class=\"bitcoin\">\n<div class=\"bitcoin-icon-wrap\">\n<span class=\"bitcoin-icon\"><i class=\"flaticon-bitcoin red\"></i></span>\n</div>\n<span class=\"bitcoin-text\"><span>Bitcoin Donate: </span><a href=\"bitcoin:3Q1337xL2i6jXrXqZ5aMfhN4wp366GQc44\">3Q1337xL2i6jXrXqZ5aMfhN4wp366GQc44</a></span>\n</div>\n<a class=\"scroll-top\" href=\"#\"><i class=\"flaticon-up\"></i></a>\n<ul>\n<li><a href=\"/\">Home</a></li>\n<li class=\"active\"><a href=\"/home/\">Full Home Page</a></li>\n<li><a href=\"/contact\">Dmca</a></li>\n<li><a href=\"/contact\">Contact</a></li>\n</ul>\n<p class=\"info\">1337x 2007 - 2022</p>\n</footer>\n<script src=\"/js/jquery-1.11.0.min.js\"></script>\n<script src=\"/js/jquery-ui.js\"></script>\n<script src=\"/js/auto-searchv2.js\"></script>\n<script src=\"/js/main.js\"></script>\n<script>\r\nif (window.top !== window.self) window.top.location.replace(window.self.location.href);\r\n</script>\n<script type=\"text/javascript\">(function(){window['__CF$cv$params']={r:'6f4e4df73ba4fe12',m:'VfTy1tZhmtaLqquuDwkuqUP7bUUqhuBvhzTCIwQhGv4-1648784439-0-AcyiFxiJtqSNsRVYLqKbX5wW90B0Whnc1eR6/x81uaj4qJ+74eMycEqF2LMEJWbk64E6epzfLc0TfEfr2Z95FnXm0jCTMvBEriMAszGmb1VRqBZdF4SVyH2oXdD1cP5TmQ==',s:[0x95f956dfcd,0xf291d9d75e],u:'/cdn-cgi/challenge-platform/h/b'}})();</script></body>\n</html>",
                    "TlsVersion": 771,
                    "CipherSuite": 49195,
                    "Certificate": "MIIFLjCCBNSgAwIBAgIQBhTy7YX2of9FwDxEyfQb1jAKBggqhkjOPQQDAjBKMQswCQYDVQQGEwJVUzEZMBcGA1UEChMQQ2xvdWRmbGFyZSwgSW5jLjEgMB4GA1UEAxMXQ2xvdWRmbGFyZSBJbmMgRUNDIENBLTMwHhcNMjEwNzA5MDAwMDAwWhcNMjIwNzA4MjM1OTU5WjB1MQswCQYDVQQGEwJVUzETMBEGA1UECBMKQ2FsaWZvcm5pYTEWMBQGA1UEBxMNU2FuIEZyYW5jaXNjbzEZMBcGA1UEChMQQ2xvdWRmbGFyZSwgSW5jLjEeMBwGA1UEAxMVc25pLmNsb3VkZmxhcmVzc2wuY29tMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEw2Ard/LZZh8zIyYzluZM+813stVbMghpUwR7cVIW1zkUzxpp6KFHXdXy11axUfv40aFFzbRtXwrJYQDZoJ3mR6OCA28wggNrMB8GA1UdIwQYMBaAFKXON+rrsHUOlGeItEX62SQQh5YfMB0GA1UdDgQWBBSBNxPW8hCKlK0p3egif26mcj5IeDA2BgNVHREELzAtgggxMzM3eC50b4IKKi4xMzM3eC50b4IVc25pLmNsb3VkZmxhcmVzc2wuY29tMA4GA1UdDwEB/wQEAwIHgDAdBgNVHSUEFjAUBggrBgEFBQcDAQYIKwYBBQUHAwIwewYDVR0fBHQwcjA3oDWgM4YxaHR0cDovL2NybDMuZGlnaWNlcnQuY29tL0Nsb3VkZmxhcmVJbmNFQ0NDQS0zLmNybDA3oDWgM4YxaHR0cDovL2NybDQuZGlnaWNlcnQuY29tL0Nsb3VkZmxhcmVJbmNFQ0NDQS0zLmNybDA+BgNVHSAENzA1MDMGBmeBDAECAjApMCcGCCsGAQUFBwIBFhtodHRwOi8vd3d3LmRpZ2ljZXJ0LmNvbS9DUFMwdgYIKwYBBQUHAQEEajBoMCQGCCsGAQUFBzABhhhodHRwOi8vb2NzcC5kaWdpY2VydC5jb20wQAYIKwYBBQUHMAKGNGh0dHA6Ly9jYWNlcnRzLmRpZ2ljZXJ0LmNvbS9DbG91ZGZsYXJlSW5jRUNDQ0EtMy5jcnQwDAYDVR0TAQH/BAIwADCCAX0GCisGAQQB1nkCBAIEggFtBIIBaQFnAHYAKXm+8J45OSHwVnOfY6V35b5XfZxgCvj5TV0mXCVdx4QAAAF6i3ixQwAABAMARzBFAiBVd/jDNSKeCa66mS/GeSTohSOfLkturC5wsXdDzbt3swIhAKDAClmoVXgjLDoNioYTpHPwUZIQgB/Plf1ec6T3PM1HAHYAQcjKsd8iRkoQxqE6CUKHXk4xixsD6+tLx2jwkGKWBvYAAAF6i3ixMwAABAMARzBFAiEApaMxT/bS7Ze1bOo2XiEP/UZwx+C/g+fFYFzIrxR5B+MCIHLweH3GxT3dedYmJfZ62Wh2sjtVAW63GMLwjx38X5ZbAHUA36Veq2iCTx9sre64X04+WurNohKkal6OOxLAIERcKnMAAAF6i3ixXQAABAMARjBEAiB3RpGRk+GsJds1lCcfu0DPoUqA6DFv2fkvDqmeXx2XZQIgCU61EV+lUuiQMoEyiyYdgP/GevUZHP6NwptXJwjkJx0wCgYIKoZIzj0EAwIDSAAwRQIgaWbSos6z7xucrvst6O9y18Th+beVBmQ1l5WwahHf4V8CIQDCMixPJrHUGenFeOHDGtxzI51YO50zl4ufbyP6n8zyWQ=="
                },
                "error": "Cipher suites do not match;Certificates do not match;Status lines do not match;Content-Length header field missing;Bodies do not match;",
                "start_time": "2022-03-31T23:40:36.767641858-04:00",
                "end_time": "2022-03-31T23:40:39.353805421-04:00"
                },
                {
                "matches_template": True,
                "control_url": "control-759c4df3ce6e8418.com",
                "start_time": "2022-03-31T23:40:44.354253273-04:00",
                "end_time": "2022-03-31T23:40:44.383259487-04:00"
                }
            ],
            "anomaly": True,
            "controls_failed": False,
            "stateful_block": False,
            "tag": "2022-03-31T23:39:30"
            }
    ]

    data = zip(data_filenames, [json.dumps(d) for d in _data])

    expected = [HyperquackRow(

    )]

    with TestPipeline() as p:
      row_lines = p | 'create data' >> beam.Create(data)
      final = hyperquack.process_hyperquack_lines(row_lines)
      beam_test_util.assert_that(final, beam_test_util.equal_to(expected))
