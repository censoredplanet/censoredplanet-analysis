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

  def __init__(
      self,
      date: datetime.date,
      caida_ip_metadata_class: Type[caida_ip_metadata.CaidaIpMetadata],
      caida_ip_metadata_bucket_folder: str,
      # pylint: disable=unused-argument
      maxmind_class: Type[maxmind.MaxmindIpMetadata],
      maxmind_bucket_folder: str,
      # pylint: enable=unused-argument
      dbip_class: Type[dbip.DbipMetadata],
      dbip_bucket_folder: str,
  ) -> None:
    """Store all metadata sources for future querying

    Args:
      date: date to initialize caida metadata at
      caida_ip_metadata_class: an IpMetadataInterface subclass (class, not instance)
      caida_ip_metadata_bucket_folder: gcs folder with CAIDA ip metadata files
      maxmind_class: an IpMetadataInterface subclass (class, not instance)
      maxmind_bucket_folder: gcs folder with maxmind files
      dbip_class: a DbipMetadata class (class, not instance)
      dbip_bucket_folder: gcs folder with dbip files
    """
    self.caida = caida_ip_metadata_class(date, caida_ip_metadata_bucket_folder,
                                         True)
    # TODO turn back on when using maxmind again.
    # self.maxmind = maxmind_class(maxmind_bucket_folder)
    self.dbip = dbip_class(dbip_bucket_folder)

  def get_metadata(self, ip: str) -> Row:
    """Pick which metadata values to return for an IP from our sources."""
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


class IpMetadataChooserFactory():

  def __init__(self, caida_file_location: str, maxmind_file_location: str,
               dbip_file_location: str) -> None:
    self.caida_file_location = caida_file_location
    self.maxmind_file_location = maxmind_file_location
    self.dbip_file_location = dbip_file_location

  def make_chooser(self, date: datetime.date) -> IpMetadataChooser:
    return IpMetadataChooser(date, caida_ip_metadata.CaidaIpMetadata,
                             self.caida_file_location,
                             maxmind.MaxmindIpMetadata,
                             self.maxmind_file_location, dbip.DbipMetadata,
                             self.dbip_file_location)


class FakeIpMetadataChooserFactory(IpMetadataChooserFactory):

  # pylint: disable=super-init-not-called
  def __init__(self) -> None:
    pass

  def make_chooser(self, date: datetime.date) -> IpMetadataChooser:
    return IpMetadataChooser(date, caida_ip_metadata.FakeCaidaIpMetadata, "",
                             maxmind.FakeMaxmindIpMetadata, "",
                             dbip.FakeDbipMetadata, "")
