"""A module which contains a number of sources of ip metadata and chooses how to add them to ips."""
import datetime
import logging

from pipeline.metadata import caida_ip_metadata
from pipeline.metadata import dbip
from pipeline.metadata.flatten import Row


# pylint: disable=too-many-instance-attributes
class IpMetadataChooser():
  """Business logic for selecting IP metadata values from a number of sources"""

  def __init__(self, caida_db: caida_ip_metadata.CaidaIpMetadata,
               dbip_db: dbip.DbipMetadata) -> None:
    """Store all metadata sources for future querying

    Args:
      caida: CAIDA database
      dbip: dbip database
    """
    self.caida = caida_db
    self.dbip = dbip_db

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
  """Factory for lazily creating an IPMetadataChooser

  Since it's too big for beam pass around as an initialized object.
  """

  def __init__(self, caida_file_location: str, maxmind_file_location: str,
               dbip_file_location: str) -> None:
    self.caida_file_location = caida_file_location
    self.maxmind_file_location = maxmind_file_location
    self.dbip_file_location = dbip_file_location

  def make_chooser(self, date: datetime.date) -> IpMetadataChooser:
    caida_db = caida_ip_metadata.CaidaIpMetadata(date, self.caida_file_location,
                                                 True)
    # TODO turn back on when using maxmind again.
    # maxmind = maxmind.MaxmindIpMetadata(self.maxmind_file_location)
    dbip_db = dbip.DbipMetadata(self.dbip_file_location)

    return IpMetadataChooser(caida_db, dbip_db)


class FakeIpMetadataChooserFactory(IpMetadataChooserFactory):

  # pylint: disable=super-init-not-called
  def __init__(self) -> None:
    pass

  def make_chooser(self, date: datetime.date) -> IpMetadataChooser:
    return IpMetadataChooser(caida_ip_metadata.FakeCaidaIpMetadata(),
                             dbip.FakeDbipMetadata())
