import unittest
from maxcarrot.message import RabbitMessage


class FunctionalTests(unittest.TestCase):
    """
    """
    def test_create_empty_message(self):
        message = RabbitMessage()
        message.prepare({'source': 'max', 'version': '0.0'})
        self.assertIn('published', message)
        self.assertEqual(message['version'], '0.0')
        self.assertEqual(message['source'], 'max')

    def test_message_from_unpacked(self):
        unpacked = {'action': 'add', 'object': 'message'}
        message = RabbitMessage(unpacked)
        self.assertEqual(message['action'], 'add')
        self.assertEqual(message['object'], 'message')

    def test_pack_message_from_unpacked(self):
        unpacked = {'action': 'add', 'object': 'message'}
        message = RabbitMessage(unpacked)
        self.assertEqual(message.packed['a'], 'a')
        self.assertEqual(message.packed['o'], 'm')

    def test_message_from_packed(self):
        packed = {'a': 'a', 'o': 'm'}
        message = RabbitMessage.unpack(packed)
        self.assertEqual(message['action'], 'add')
        self.assertEqual(message['object'], 'message')
