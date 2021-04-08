"""Filenames for analysis assets."""
import os.path

root = os.path.dirname(os.path.abspath(__file__))
MAXMIND_CITY = os.path.join(root, 'assets/GeoLite2-City.mmdb')
MAXMIND_ASN = os.path.join(root, 'assets/GeoLite2-ASN.mmdb')
FALSE_POSITIVES = os.path.join(root, 'assets/false_positive_signatures.json')
BLOCKPAGES = os.path.join(root, 'assets/blockpage_signatures.json')
COUNTRY_CODES = os.path.join(root, 'assets/country_codes.json')
