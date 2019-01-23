import sys

import yaml


yaml_invalid = yaml.reader.Reader.NON_PRINTABLE

for line in sys.stdin:
    sys.stdout.write(yaml_invalid.sub("", line))
