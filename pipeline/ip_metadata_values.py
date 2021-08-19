"""A module to hold a bunch of values needed to init an IPMetadataChooser."""
from typing import Type

import firehook_resources
from pipeline.metadata import caida_ip_metadata
from pipeline.metadata import dbip
from pipeline.metadata import maxmind


class IpMetadataValuesInterface():
  """Interface for passing around metadata prod values"""

  def __init__(self) -> None:
    # Set types correctly for interface
    self.caida_ip_metadata_class: Type[
        caida_ip_metadata.CaidaIpMetadataInterface]
    self.caida_ip_metadata_bucket_folder: str
    self.maxmind_clas: Type[maxmind.MaxmindIpMetadataInterface]
    self.maxmind_bucket_folder: str
    self.dbip_class: Type[dbip.DbipIpMetadataInterface]
    self.dbip_bucket_folder: str


class IpMetadataProdValues(IpMetadataValuesInterface):
  """Production values for IP metadata"""

  def __init__(self) -> None:
    super().__init__()
    # Because an instantiated CaidaIpMetadata object is too big for beam's
    # serlalization to pass around we pass in the class to instantiate instead.
    self.caida_ip_metadata_class = caida_ip_metadata.CaidaIpMetadata
    self.caida_ip_metadata_bucket_folder = firehook_resources.CAIDA_FILE_LOCATION
    # Maxmind is also too big to pass around
    self.maxmind_class = maxmind.MaxmindIpMetadata
    self.maxmind_bucket_folder = firehook_resources.MAXMIND_FILE_LOCATION
    # DBIP is also too big to pass around
    self.dbip_class = dbip.DbipMetadata
    self.dbip_bucket_folder = firehook_resources.DBIP_FILE_LOCATION


class IpMetadataFakeValues(IpMetadataValuesInterface):
  """Fake IpMetadata values for use in tests."""

  def __init__(self) -> None:
    super().__init__()
    self.caida_ip_metadata_class = caida_ip_metadata.FakeCaidaIpMetadata
    self.caida_ip_metadata_bucket_folder = ""
    self.maxmind_class = maxmind.FakeMaxmindIpMetadata
    self.maxmind_bucket_folder = ""
    self.dbip_class = dbip.FakeDbipMetadata
    self.dbip_bucket_folder = ""
