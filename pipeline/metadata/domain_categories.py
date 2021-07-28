"""Get the category for a domain.

Categories:
  Alcohol & Drugs
  Anonymization and circumvention tools
  Communication Tools
  Culture
  E-commerce
  Economics
  Environment
  File-sharing
  Foreign relations and military
  Gambling
  Gaming
  Government
  Hacking Tools
  Hate Speech
  History arts and literature
  Hosting and Blogging Platforms
  Human Rights Issues
  Illegal
  Intergovernmental Organizations
  LGBT
  Media sharing
  Miscelaneous content
  News Media
  Online Dating
  Political Criticism
  Pornography
  Provocative Attire
  Public Health
  Religion
  Search Engines
  Sex Education
  Social Networking
  Terrorism and Militants
"""

import io
import pkgutil
from typing import Optional, Dict
from urllib.parse import urlparse

DOMAIN_CATEGORIES = 'data/domain_categories.csv'


def _load_categories(filepath: str) -> Dict[str, str]:
  """Load data for domain category matching.

  Args:
    filepath: relative path to csv file containing domains and categories

  Returns:
    Dictionary mapping domains to categories
  """
  data = pkgutil.get_data(__name__, filepath)
  if not data:
    raise FileNotFoundError(f"Couldn't find file {filepath}")
  content = io.TextIOWrapper(io.BytesIO(data), encoding='utf-8')

  categories = {}
  for line in content.readlines():
    domain, category = line.strip().split(',')
    categories[domain] = category
  return categories


class DomainCategoryMatcher:
  """Matcher to find categories for domains."""

  def __init__(self) -> None:
    self.categories = _load_categories(DOMAIN_CATEGORIES)

  def match_url(self, url: str) -> Optional[str]:
    """Return the category for a url if known.

    Args:
      url: string of the form
       "www.google.com", "1.1.1.1", or "test.com/path"

    Returns: a string category like "Online Dating" or None
    """
    domain = urlparse("http://" + url).netloc
    return self.categories.get(domain, None)
