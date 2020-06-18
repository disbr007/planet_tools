import json

from planet import api
from planet.api import filters


#### Constants
# Redundant? Is three band just reduced 4-band or diff sensors?
dove_item_types = ['PSScene3Band', 'PSScene4Band']

#### Set up
# Retrieves API key from PL_API_KEY env. var.
client = api.ClientV1()


def p(data):
    print(json.dumps(data, indent=2))


