# -*- coding: utf-8 -*-
import unittest


from maxcarrot import RabbitClient

RABBIT_URL = "amqp://guest:guest@localhost:5672"
TEST_VHOST_URL = '{}/tests'.format(RABBIT_URL)
RABBIT_MANAGEMENT_URL = "http://localhost:15672/api".format(RABBIT_URL)


class RabbitTests(unittest.TestCase):

    def setUp(self):
        self.server = RabbitClient(TEST_VHOST_URL)
        self.server.management.cleanup(delete_all=True)
        self.server.declare()
        self.clients = {}

    def tearDown(self):
        self.server.disconnect()
        for user_clients in self.clients.values():
            for client in user_clients:
                client.disconnect()

    def getClient(self, username, reuse=True):
        self.clients.setdefault(username, [])
        if not self.clients[username] or not reuse:
            self.clients[username].append(RabbitClient(TEST_VHOST_URL, user=username))
        if reuse:
            return self.clients[username][0]
        else:
            return self.clients[username][-1]
