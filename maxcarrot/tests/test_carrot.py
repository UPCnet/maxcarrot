import unittest
import requests
import json

from maxcarrot import RabbitServer, RabbitClient

RABBIT_URL = "localhost:5672"
TEST_VHOST_URL = '{}/tests'.format(RABBIT_URL)
RABBIT_MANAGEMENT_URL = "http://localhost:15672/api".format(RABBIT_URL)


class FunctionalTests(unittest.TestCase):

    def setUp(self):
        self.cleanup()
        self.server = RabbitServer(TEST_VHOST_URL, 'guest', 'guest', bind=False)
        self.clients = {}

    def tearDown(self):
        self.server.disconnect()
        for client in self.clients.values():
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

    def getClient(self, username):
        client = self.clients.setdefault(username, RabbitClient(TEST_VHOST_URL, username, ''))
        return client

    # BEGIN TESTS

    def test_basic_send_receive(self):
        """
        Given two users in a conversation
        When each of them writes a message
        Then each of them receives both messages
        """

        self.server.conversations.create('conversation1', users=['sheldon', 'leonard'])

        sheldon = self.getClient('sheldon')
        leonard = self.getClient('leonard')

        sheldon.conversations.send('conversation1', 'Hello!')
        leonard.conversations.send('conversation1', 'Hello...')

        messages_to_sheldon = sheldon.conversations.get_all('conversation1')
        messages_to_leonard = leonard.conversations.get_all('conversation1')

        self.assertEqual(len(messages_to_sheldon), 2)
        self.assertEqual(len(messages_to_leonard), 2)
