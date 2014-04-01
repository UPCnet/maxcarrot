from maxcarrot.tests import RabbitTests
from rabbitpy.exceptions import AMQPNotFound


class FunctionalTests(RabbitTests):
    # BEGIN TESTS

    def test_basic_send_receive(self):
        """
        Given two users in a conversation
        When each of them writes a message
        Then each of them receives both messages
        """
        self.server.create_users(['sheldon', 'leonard'])
        self.server.conversations.create('conversation1', users=['sheldon', 'leonard'])

        sheldon = self.getClient('sheldon')
        leonard = self.getClient('leonard')

        sheldon.conversations.send('conversation1', 'Hello!')
        leonard.conversations.send('conversation1', 'Hello...')

        messages_to_sheldon = sheldon.conversations.get_all('conversation1')
        messages_to_leonard = leonard.conversations.get_all('conversation1')

        self.assertEqual(len(messages_to_sheldon), 2)
        self.assertEqual(len(messages_to_leonard), 2)

    def test_basic_send_receive_all(self):
        """
        Given two users in a conversation
        And one of the users has more than one consumer
        When each of them writes a message
        Then all client consumers receives both messages
        """
        self.server.create_users(['sheldon', 'leonard'])
        self.server.conversations.create('conversation1', users=['sheldon', 'leonard'])

        sheldon = self.getClient('sheldon')
        leonard = self.getClient('leonard')
        leonard2 = self.getClient('leonard', reuse=False)

        sheldon.conversations.send('conversation1', 'Hello!')
        leonard.conversations.send('conversation1', 'Hello...')

        messages_to_sheldon = sheldon.conversations.get_all('conversation1')
        messages_to_leonard = leonard.conversations.get_all('conversation1')
        messages_to_leonard2 = leonard2.conversations.get_all('conversation1')

        self.assertEqual(len(messages_to_sheldon), 2)
        self.assertEqual(len(messages_to_leonard), 2)
        self.assertEqual(len(messages_to_leonard2), 2)

    def test_messages_queue_receive(self):
        """
        Given a conversation
        When a message is posted
        Then the messages queue receives the message
        And the user also receives the message
        """
        self.server.create_users(['sheldon', 'leonard'])
        self.server.conversations.create('conversation1', users=['sheldon', 'leonard'])

        sheldon = self.getClient('sheldon')
        sheldon.conversations.send('conversation1', 'Hello!')
        messages_to_sheldon = sheldon.conversations.get_all('conversation1')
        messages_to_queue = self.server.get_all('messages')

        self.assertEqual(len(messages_to_sheldon), 1)
        self.assertEqual(len(messages_to_queue), 1)

    def test_messages_queue_receive_all(self):
        """
        Given two conversation
        When a message is posted in each conversation
        Then the messages queue receives all the messages
        """
        self.server.create_users(['sheldon', 'leonard'])
        self.server.create_users(['penny', 'howard'])
        self.server.conversations.create('conversation1', users=['sheldon', 'leonard'])
        self.server.conversations.create('conversation2', users=['penny', 'howard'])

        sheldon = self.getClient('sheldon')
        penny = self.getClient('penny')

        sheldon.conversations.send('conversation1', 'Hello!')
        penny.conversations.send('conversation2', 'Hello!')

        messages_to_sheldon = sheldon.conversations.get_all('conversation1')
        messages_to_penny = penny.conversations.get_all('conversation2')
        messages_to_queue = self.server.get_all('messages')

        self.assertEqual(len(messages_to_sheldon), 1)
        self.assertEqual(len(messages_to_penny), 1)
        self.assertEqual(len(messages_to_queue), 2)

    def test_drop_messages_to_unbind_conversation(self):
        """
        Given two conversations
        When a message is posted in a conversation the user isn't binded
        Then the messages don't get anywhere
        """
        self.server.create_users(['sheldon', 'leonard'])
        self.server.create_users(['penny', 'howard'])
        self.server.conversations.create('conversation1', users=['sheldon', 'leonard'])
        self.server.conversations.create('conversation2', users=['penny', 'howard'])

        sheldon = self.getClient('sheldon')
        penny = self.getClient('penny')

        sheldon.conversations.send('conversation2', 'Hello!')

        messages_to_sheldon = sheldon.conversations.get_all('conversation2')
        messages_to_penny = penny.conversations.get_all('conversation2')
        messages_to_queue = self.server.get_all('messages')

        self.assertEqual(len(messages_to_sheldon), 0)
        self.assertEqual(len(messages_to_penny), 0)
        self.assertEqual(len(messages_to_queue), 0)

    def test_drop_messages_from_unbind_user(self):
        """
        Given two users in a conversation
        When one of them gets unbinded from the conversation
        Then messages from the unbinded user go nowhere
        And messages from the still binded user don't reach unbinded user
        """
        self.server.create_users(['sheldon', 'leonard'])
        self.server.conversations.create('conversation1', users=['sheldon', 'leonard'])

        sheldon = self.getClient('sheldon')
        leonard = self.getClient('leonard')

        self.server.conversations.unbind_user('conversation1', 'sheldon')

        sheldon.conversations.send('conversation1', 'Hello!')
        leonard.conversations.send('conversation1', 'Hello...')

        messages_to_sheldon = sheldon.conversations.get_all('conversation1')
        messages_to_leonard = leonard.conversations.get_all('conversation1')

        self.assertEqual(len(messages_to_sheldon), 0)
        self.assertEqual(len(messages_to_leonard), 1)

    def test_create_user(self):
        """
        Given two users in a conversation
        When one of them gets removed from the system
        Then it's associated exchanges disappear
        """
        self.server.create_users(['sheldon', 'leonard'])

        self.assertIsNotNone(self.get_exchange('sheldon.publish'))
        import ipdb;ipdb.set_trace()
        self.assertIsNotNone(self.get_exchange('sheldon.subscribe'))

    def test_delete_user(self):
        """
        Given two users in a conversation
        When one of them gets removed from the system
        Then it's associated exchanges disappear
        """
        self.server.create_users(['sheldon', 'leonard'])
        self.server.conversations.create('conversation1', users=['sheldon', 'leonard'])

        self.server.delete_user('sheldon')

        self.assertIsNone(self.get_exchange('sheldon.publish'))
        self.assertIsNone(self.get_exchange('sheldon.subscribe'))
