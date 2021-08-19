"""A module which contains a number of sources of ip metadata and chooses how to add them to ips."""
import datetime
import logging

from pipeline.metadata.flatten import Row
from pipeline.ip_metadata_values import IpMetadataValuesInterface


class IpMetadataChooser():
  """Business logic for selecting IP metadata values from a number of sources"""

  def __init__(self, date: datetime.date,
               prod_values: IpMetadataValuesInterface) -> None:
    self.caida = prod_values.caida_ip_metadata_class(
        date, prod_values.caida_ip_metadata_bucket_folder, True)
    # TODO turn back on when using maxmind again.
    # self.maxmind = prod_values.maxmind_class(prod_values.maxmind_bucket_folder)
    self.dbip = prod_values.dbip_class(prod_values.dbip_bucket_folder)

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
