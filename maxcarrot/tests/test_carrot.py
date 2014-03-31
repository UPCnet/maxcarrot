import unittest

from maxcarrot import RabbitServer, RabbitClient

RABBIT_URL = "localhost:5672/tests"


class FunctionalTests(unittest.TestCase):

    def setUp(self):
        pass

    # BEGIN TESTS

    def test_basic_send_receive(self):
        """

        """
        server = RabbitServer(RABBIT_URL, 'guest', 'guest', bind=False)
        server.conversations.create('conversation1', users=['sheldon', 'leonard'])
        server.disconnect()

        sheldon = RabbitClient(RABBIT_URL, 'sheldon', '')
        leonard = RabbitClient(RABBIT_URL, 'leonard', '')

        sheldon.conversations.send('conversation1', 'Hello!')
        leonard.conversations.send('conversation1', 'Hello...')

        messages_to_sheldon = sheldon.conversations.get_all('conversation1')
        messages_to_leonard = leonard.conversations.get_all('conversation1')

        sheldon.disconnect()
        leonard.disconnect()

        self.assertEqual(len(messages_to_sheldon), 2)
        self.assertEqual(len(messages_to_leonard), 2)
