from maxcarrot.tests import RabbitTests


class FunctionalTests(RabbitTests):
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

    def test_basic_send_receive_all(self):
        """
        Given two users in a conversation
        And one of the users has more than one consumer
        When each of them writes a message
        Then all client consumers receives both messages
        """

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
