"""Test domain category lookup."""

import unittest

from pipeline.metadata import domain_categories


class BlockpageTest(unittest.TestCase):
  """Tests for the blockpage matcher."""

  def test_no_category(self) -> None:
    matcher = domain_categories.DomainCategoryMatcher()
    self.assertIsNone(matcher.match_url('site-with-no-category.com'))

  def test_match_category(self) -> None:
    matcher = domain_categories.DomainCategoryMatcher()
    self.assertEqual('LGBT', matcher.match_url('amnestyusa.org'))

  def test_match_subdomain(self) -> None:
    matcher = domain_categories.DomainCategoryMatcher()
    self.assertEqual('Gaming', matcher.match_url('na.leagueoflegends.com'))

  def test_match_non_com_tld(self) -> None:
    matcher = domain_categories.DomainCategoryMatcher()
    self.assertEqual('Economics', matcher.match_url('dfid.gov.uk'))

  def test_match_ip(self) -> None:
    matcher = domain_categories.DomainCategoryMatcher()
    self.assertEqual('Hosting and Blogging Platforms',
                     matcher.match_url('9.9.9.9'))

  def test_match_url(self) -> None:
    matcher = domain_categories.DomainCategoryMatcher()
    self.assertEqual('LGBT',
                     matcher.match_url('76crimes.com/anti-lgbt-laws-malaysia/'))

  def test_match_ip_url(self) -> None:
    matcher = domain_categories.DomainCategoryMatcher()
    self.assertEqual('Hosting and Blogging Platforms',
                     matcher.match_url('1.0.0.1/dns/'))
