import unittest
from pprint import pprint
from collections import Counter
from hashlib import sha256

from pipeline.lookup_domain import DOMAINS, domain_partition, DOMAIN_PARTITIONS

class DopmainTest(unittest.TestCase):
  """Unit tests for domaion bucketing."""

  # pylint: disable=protected-access

  def test_domain_bucketing(self) -> None:
    partitions = Counter(list(map(domain_partition, DOMAINS)))
    pprint("initial domain partitions)")
    pprint(partitions)

  def test_hash_domain_bucketing(self) -> None:
    partitions = Counter(list(map(lambda x: hash(x) % DOMAIN_PARTITIONS, DOMAINS)))
    pprint("hashed domain partitions)")
    pprint(partitions)

  def test_double_hash_domain_bucketing(self) -> None:
    partitions = Counter(list(map(lambda x: hash(x) % 200, DOMAINS)))
    pprint("200 bucket hashed domain partitions)")
    pprint(partitions)

  def test_sha256_domain_bucketing(self) -> None:
    partitions = Counter(list(map(lambda x: int.from_bytes(sha256(x.encode('utf-8')).digest(), 'big') % DOMAIN_PARTITIONS, DOMAINS)))
    pprint("200 sha256 hashed domain partitions)")
    pprint(partitions)