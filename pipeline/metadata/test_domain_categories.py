"""Test domain category lookup."""

import unittest

from pipeline.metadata import domain_categories


class BlockpageTest(unittest.TestCase):
  """Tests for the blockpage matcher."""

  def test_no_category(self) -> None:
    matcher = domain_categories.DomainCategoryMatcher()
    self.assertIsNone(matcher.get_category('site-with-no-category.com', False))

  def test_match_category(self) -> None:
    matcher = domain_categories.DomainCategoryMatcher()
    self.assertEqual('LGBT', matcher.get_category('amnestyusa.org', False))

  def test_match_subdomain(self) -> None:
    matcher = domain_categories.DomainCategoryMatcher()
    self.assertEqual('Gaming',
                     matcher.get_category('na.leagueoflegends.com', False))

  def test_match_non_com_tld(self) -> None:
    matcher = domain_categories.DomainCategoryMatcher()
    self.assertEqual('Economics', matcher.get_category('dfid.gov.uk', False))

  def test_match_ip(self) -> None:
    matcher = domain_categories.DomainCategoryMatcher()
    self.assertEqual('Hosting and Blogging Platforms',
                     matcher.get_category('9.9.9.9', False))

  def test_match_url(self) -> None:
    matcher = domain_categories.DomainCategoryMatcher()
    self.assertEqual(
        'LGBT',
        matcher.get_category('76crimes.com/anti-lgbt-laws-malaysia/', False))

  def test_match_ip_url(self) -> None:
    matcher = domain_categories.DomainCategoryMatcher()
    self.assertEqual('Hosting and Blogging Platforms',
                     matcher.get_category('1.0.0.1/dns/', False))

  def test_match_control(self) -> None:
    matcher = domain_categories.DomainCategoryMatcher()
    self.assertEqual('Control', matcher.get_category('example.com', True))
