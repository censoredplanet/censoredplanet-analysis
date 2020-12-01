import os.path

root = os.path.dirname(os.path.abspath(__file__))
MAXMIND = os.path.join(root, 'assets/GeoLite2-City.mmdb')
FALSE_POSITIVES = os.path.join(root, 'assets/false_positive_signatures.json')
BLOCKPAGES = os.path.join(root, 'assets/blockpage_signatures.json')
