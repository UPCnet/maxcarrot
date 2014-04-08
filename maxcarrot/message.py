import pkg_resources
import json
from copy import deepcopy
import datetime
from pprint import pformat

# Load specification and make an inverted copy
SPECIFICATION = json.loads(open(pkg_resources.resource_filename(__name__, 'specification.json')).read())
_SPECIFICATION = {v['id']: {'name': k, 'type': v['type'], 'values': {vv['id']: {'name': kk} for kk, vv in v.get('values', {}).items()}} for k, v in SPECIFICATION.items()}


class RabbitMessage(dict):
    def __init__(self, message=None):
        """
        """
        if message is not None:
            self.clear()
            self.update(deepcopy(message))

    def __repr__(self):
        return pformat(dict(self))

    def __getattr__(self, key):
        return self[key]

    def prepare(self, params={}):
        self['published'] = datetime.datetime.utcnow()
        self.update(params)

    def is_packed(self, message):
        return sum([len(k) for k in message.keys()]) == len(message.keys())

    @property
    def packed(self):
        packed = {}

        for field, value in self.items():
            if field in SPECIFICATION:
                spec = SPECIFICATION[field]
                if spec.get('values', {}):
                    packed_value = spec['values'].get(value, None)
                    if packed_value is not None:
                        packed[spec['id']] = packed_value['id']
                else:
                    packed[spec['id']] = value

        return packed

    @classmethod
    def unpack(self, packed):
        unpacked = {}

        for field, value in packed.items():
            if field in _SPECIFICATION:
                spec = _SPECIFICATION[field]
                if spec.get('values', {}):
                    packed_value = None
                    if isinstance(value, str) or isinstance(value, unicode):
                        packed_value = spec['values'].get(value, None)
                    if packed_value is not None:
                        unpacked[spec['name']] = packed_value['name']
                else:
                    unpacked[spec['name']] = value
        message = RabbitMessage(unpacked)
        return message
