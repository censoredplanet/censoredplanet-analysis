"""A module which contains a number of sources of ip metadata and chooses how to add them to ips."""
import datetime
import logging
from typing import Type

from pipeline.metadata import caida_ip_metadata
from pipeline.metadata import dbip
from pipeline.metadata import maxmind
from pipeline.metadata.flatten import Row


# pylint: disable=too-many-instance-attributes
class IpMetadataChooser():
  """Business logic for selecting IP metadata values from a number of sources"""

  def __init__(self, caida_ip_metadata_class: Type[
      caida_ip_metadata.CaidaIpMetadataInterface],
               caida_ip_metadata_bucket_folder: str,
               maxmind_class: Type[maxmind.MaxmindIpMetadataInterface],
               maxmind_bucket_folder: str,
               dbip_class: Type[dbip.DbipIpMetadataInterface],
               dbip_bucket_folder: str) -> None:
    """Store all the values needed to setup ip metadata databases.

    Args:
      caida_ip_metadata_class: an IpMetadataInterface subclass (class, not instance)
      caida_ip_metadata_bucket_folder: gcs folder with CAIDA ip metadata files
      maxmind_class: an IpMetadataInterface subclass (class, not instance)
      maxmind_bucket_folder: gcs folder with maxmind files
      dbip_class: a DbipMetadata class (class, not instance)
      dbip_bucket_folder: gcs folder with dbip files

    Objects of this class cannot be used until .fully_init() is called
    """
    self.caida_ip_metadata_class = caida_ip_metadata_class
    self.caida_ip_metadata_bucket_folder = caida_ip_metadata_bucket_folder
    self.maxmind_class = maxmind_class
    self.maxmind_bucket_folder = maxmind_bucket_folder
    self.dbip_class = dbip_class
    self.dbip_bucket_folder = dbip_bucket_folder

    self.fully_initialized = False

  # pylint: disable=attribute-defined-outside-init
  def fully_init(self, date: datetime.date) -> None:
    """Fully instantiate all databases for ip metadata.

    This needs to be done lazily because otherwise the
    object is too big for beam to pass around.
    """
    self.caida = self.caida_ip_metadata_class(
        date, self.caida_ip_metadata_bucket_folder, True)
    # TODO turn back on when using maxmind again.
    # self.maxmind = self.maxmind_class(self.maxmind_bucket_folder)
    self.dbip = self.dbip_class(self.dbip_bucket_folder)

    self.fully_initialized = True

  def get_metadata(self, ip: str) -> Row:
    """Pick which metadata values to return for an IP from our sources."""
    if not self.fully_initialized:
      raise Exception(
          "IpMetadataChooser was not fully initialized before getting metadata."
      )

    try:
      (netblock, asn, as_name, as_full_name, as_type,
       country) = self.caida.lookup(ip)
      metadata_values = {
          'netblock': netblock,
          'asn': asn,
          'as_name': as_name,
          'as_full_name': as_full_name,
          'as_class': as_type,
          'country': country,
      }

      (org, dbip_asn) = self.dbip.lookup(ip)
      if org and asn == dbip_asn:
        # Since we're currently using a single dated dbip table
        # which becomes less accurate for past data
        # we're doing the simple thing here and only including organization info
        # if the AS from dbip matches the AS from CAIDA
        metadata_values['organization'] = org

      # Turning off maxmind data for now.
      # if not metadata_values['country']:  # try Maxmind
      #   (netblock, asn, as_name, as_full_name, as_type,
      #    country) = maxmind_db.lookup(ip)
      #   metadata_values['country'] = country
    except KeyError as e:
      logging.warning('KeyError: %s\n', e)
      metadata_values = {}  # values are missing, but entry should still exist

    return metadata_values


def get_prod_ip_metadata_chooser(caida_file_location: str,
                                 maxmind_file_location: str,
                                 dbip_file_location: str) -> IpMetadataChooser:
  return IpMetadataChooser(caida_ip_metadata.CaidaIpMetadata,
                           caida_file_location, maxmind.MaxmindIpMetadata,
                           maxmind_file_location, dbip.DbipMetadata,
                           dbip_file_location)


def get_fake_ip_metadata_chooser() -> IpMetadataChooser:
  return IpMetadataChooser(caida_ip_metadata.FakeCaidaIpMetadata, "",
                           maxmind.FakeMaxmindIpMetadata, "",
                           dbip.FakeDbipMetadata, "")
