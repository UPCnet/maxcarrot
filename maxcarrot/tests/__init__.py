# -*- coding: utf-8 -*-
import unittest
import requests
import json

from maxcarrot import RabbitServer, RabbitClient

RABBIT_URL = "localhost:5672"
TEST_VHOST_URL = '{}/tests'.format(RABBIT_URL)
RABBIT_MANAGEMENT_URL = "http://localhost:15672/api".format(RABBIT_URL)


class RabbitTests(unittest.TestCase):

    def setUp(self):
        self.cleanup()
        self.server = RabbitServer(TEST_VHOST_URL, 'guest', 'guest', bind=False)
        self.clients = {}

    def tearDown(self):
        self.server.disconnect()
        for user_clients in self.clients.values():
            for client in user_clients:
                client.disconnect()

    def cleanup(self):
        # Delete all exchanges except rabbitmq default ones
        req = requests.get('{}/exchanges/tests'.format(RABBIT_MANAGEMENT_URL), auth=('guest', 'guest'))
        for exchange in req.json():
            if not(exchange['name'] and exchange['name'].startswith('amq.')):
                requests.delete(
                    '{}/exchanges/tests/{}'.format(RABBIT_MANAGEMENT_URL, exchange['name']),
                    data=json.dumps({'vhost': 'tests', 'name': exchange['name']}),
                    auth=('guest', 'guest')
                )

        # Delete all exchanges except dynamic ones
        req = requests.get('{}/queues/tests'.format(RABBIT_MANAGEMENT_URL), auth=('guest', 'guest'))
        for queue in req.json():

            if not queue['name'].startswith('amq.gen'):

                requests.delete(
                    '{}/queues/tests/{}'.format(RABBIT_MANAGEMENT_URL, queue['name']),
                    data=json.dumps({'mode': 'delete', 'vhost': 'tests', 'name': queue['name']}),
                    auth=('guest', 'guest')
                )

    def getClient(self, username, reuse=True):
        self.clients.setdefault(username, [])
        if not self.clients[username] or not reuse:
            self.clients[username].append(RabbitClient(TEST_VHOST_URL, username, ''))
        if reuse:
            return self.clients[username][0]
        else:
            return self.clients[username][-1]
