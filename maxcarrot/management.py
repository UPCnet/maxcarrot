import requests
import re
from itertools import chain


class RabbitManagement(object):
    def __init__(self, client, url, vhost, user, password):
        self.url = url
        self.vhost = vhost.replace('%2F', '/')
        self.vhost_url = vhost
        self.user = user
        self.password = password
        self.client = client

        self.auth = (self.user, self.password)

        self.exchanges = []
        self.queues = []
        self.exchanges_by_name = {}
        self.queues_by_name = {}

    def delete_exchange(self, name):
        self.client.ch.exchange.delete(name)

    def delete_queue(self, name):
        if not name.startswith('amq.gen'):
            self.client.ch.queue.delete(name)

    def load_exchanges(self):
        req = requests.get('{}/exchanges/{}'.format(self.url, self.vhost_url), auth=self.auth)
        self.exchanges = [a for a in req.json() if a['vhost'] == self.vhost]
        self.exchanges_by_name.clear()
        for exchange in self.exchanges:
            self.exchanges_by_name[exchange['name']] = exchange

    def load_queues(self):
        req = requests.get('{}/queues/{}'.format(self.url, self.vhost_url), auth=self.auth)
        self.queues = [a for a in req.json() if a['vhost'] == self.vhost]
        self.queues_by_name.clear()
        for queue in self.queues:
            self.queues_by_name[queue['name']] = queue

    def load_exchange_bindings(self, exchange):
        resp_source = requests.get('{}/exchanges/{}/{}/bindings/source'.format(self.url, self.vhost_url, exchange), auth=self.auth)
        resp_destination = requests.get('{}/exchanges/{}/{}/bindings/destination'.format(self.url, self.vhost_url, exchange), auth=self.auth)

        source_bindings = resp_source.json() if resp_source.status_code == 200 else []
        destination_bindings = resp_destination.json() if resp_destination.status_code == 200 else []
        return chain(source_bindings, destination_bindings)

    def cleanup(self, delete_all=False):
        self.load_exchanges()
        self.load_queues()
        for exchange in self.exchanges:
            matched_definition = False

            for exchange_definition in self.client.resource_specs['exchanges']:
                if not matched_definition and re.match(exchange_definition['spec'], exchange['name']):
                    matched_definition = True
                    exchange['native'] = exchange_definition.get('native', False)
                    types_match = re.match(exchange_definition['type'], exchange['type'])
                    autodelete_match = exchange_definition.get('auto_delete', False) == exchange['auto_delete']
                    durable_match = exchange_definition.get('durable', True) == exchange['durable']
                    if not types_match or not autodelete_match or not durable_match:
                        print 'Deleting non matching exchange "{name}"'.format(**exchange)
                        self.delete_exchange(exchange['name'])

            if not matched_definition or (delete_all and not exchange.get('native', False)):
                self.delete_exchange(exchange['name'])

        for queue in self.queues:
            matched_definition = False
            for queue_definition in self.client.resource_specs['queues']:
                if not matched_definition and re.match(queue_definition['spec'], queue['name']):
                    matched_definition = True
                    queue['native'] = queue_definition.get('native', False)
                    autodelete_match = queue_definition.get('auto_delete', False) == queue['auto_delete']
                    durable_match = queue_definition.get('durable', True) == queue['durable']
                    if not autodelete_match or not durable_match:
                        print 'Deleting non maching queue "{name}"'.format(**queue)
                        self.delete_queue(queue['name'])

            if not matched_definition or (delete_all and not exchange.get('native', False)):
                self.delete_queue(queue['name'])
