import pkg_resources
import json
from copy import deepcopy
import datetime
from rfc3339 import rfc3339
from pprint import pformat
from uuid import uuid1

# Load specification and make an inverted copy
SPECIFICATION = json.loads(open(pkg_resources.resource_filename(__name__, 'specification.json')).read())
_SPECIFICATION = {}

for k, v in SPECIFICATION.items():
    spec_id = v['id']
    spec_value = {
        'name': k,
        'type': v['type']
    }
    for kk, vv in v.get('values', {}).items():
        spec_value.setdefault('values', {})
        spec_value['values'][vv['id']] = {'name': kk}

    for kk, vv in v.get('fields', {}).items():
        spec_value.setdefault('fields', {})
        spec_value['fields'][vv['id']] = {'name': kk}

    _SPECIFICATION[v['id']] = spec_value


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
        self['published'] = rfc3339(datetime.datetime.utcnow(), utc=True, use_system_timezone=False)
        self['uuid'] = str(uuid1())
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
                    if spec.get('fields', {}) and spec['type'] == 'object' and isinstance(value, dict):
                        packed_inner = {}
                        for inner_field, inner_value in value.items():
                            if spec['fields'].get(inner_field, None):
                                packed_key = spec['fields'][inner_field]['id']
                            else:
                                packed_key = inner_field
                            packed_inner[packed_key] = inner_value
                        packed[spec['id']] = packed_inner
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

                    if spec.get('fields', {}) and spec['type'] == 'object' and isinstance(value, dict):
                        unpacked_inner = {}
                        for inner_field, inner_value in value.items():
                            if spec['fields'].get(inner_field, None):
                                packed_key = spec['fields'][inner_field]['name']
                            else:
                                packed_key = inner_field
                            unpacked_inner[packed_key] = inner_value
                        unpacked[spec['name']] = unpacked_inner
        message = RabbitMessage(unpacked)
        return message
