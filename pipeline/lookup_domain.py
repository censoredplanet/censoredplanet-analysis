"""Module to lookup domain partition for Satellite pipeline."""

DOMAIN_PARTITIONS = 250


def domain_partition(domain: str) -> int:
  return hash(domain) % DOMAIN_PARTITIONS
