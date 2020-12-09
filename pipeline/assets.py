import os.path

root = os.path.dirname(os.path.abspath(__file__))
MAXMIND_CITY = os.path.join(root, 'assets/maxmind/GeoLite2-City.mmdb')
MAXMIND_ASN = os.path.join(root, 'assets/maxmind/GeoLite2-ASN.mmdb')
FALSE_POSITIVES = os.path.join(root, 'assets/internal/false_positive_signatures.json')
BLOCKPAGES = os.path.join(root, 'assets/internal/blockpage_signatures.json')
