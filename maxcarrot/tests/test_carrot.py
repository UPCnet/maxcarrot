from maxcarrot.tests import RabbitTests


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

        sheldon.send('conversation1', 'Hello!')
        leonard.send('conversation1', 'Hello...')

        messages_to_sheldon = sheldon.get_all()
        messages_to_leonard = leonard.get_all()

        self.assertEqual(len(messages_to_sheldon), 2)
        self.assertEqual(len(messages_to_leonard), 2)

    def test_internal_message(self):
        """
        Given two users without conversations
        And one of them has multiple instances
        When one user send an internal message
        Then only the instances of that user receives the message

        """
        self.server.create_users(['sheldon', 'leonard'])

        sheldon = self.getClient('sheldon')
        sheldon1 = self.getClient('sheldon', reuse=False)
        sheldon2 = self.getClient('sheldon', reuse=False)
        leonard = self.getClient('leonard')

        sheldon.send('internal', 'Hello!')

        messages_to_sheldon = sheldon.get_all()
        messages_to_sheldon1 = sheldon1.get_all()
        messages_to_sheldon2 = sheldon2.get_all()
        messages_to_leonard = leonard.get_all()

        self.assertEqual(len(messages_to_sheldon), 1)
        self.assertEqual(len(messages_to_sheldon1), 1)
        self.assertEqual(len(messages_to_sheldon2), 1)
        self.assertEqual(len(messages_to_leonard), 0)

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

        sheldon.send('conversation1', 'Hello!')
        leonard.send('conversation1', 'Hello...')

        messages_to_sheldon = sheldon.get_all()
        messages_to_leonard = leonard.get_all()
        messages_to_leonard2 = leonard2.get_all()

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
        sheldon.send('conversation1', 'Hello!')
        messages_to_sheldon = sheldon.get_all()
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

        sheldon.send('conversation1', 'Hello!')
        penny.send('conversation2', 'Hello!')

        messages_to_sheldon = sheldon.get_all()
        messages_to_penny = penny.get_all()
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

        sheldon.send('conversation2', 'Hello!')

        messages_to_sheldon = sheldon.get_all()
        messages_to_penny = penny.get_all()
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

        sheldon.send('conversation1', 'Hello!')
        leonard.send('conversation1', 'Hello...')

        messages_to_sheldon = sheldon.get_all()
        messages_to_leonard = leonard.get_all()

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

    # def test_unread(self):
    #     """
    #     Given two users in a conversation
    #     When one of them sends a message
    #     And the other one is not listening
    #     Then the message ends in the unread queue
    #     """
    #     self.server.create_users(['sheldon', 'leonard'])
    #     self.server.conversations.create('conversation1', users=['sheldon', 'leonard'])

    #     sheldon = self.getClient('sheldon')
    #     sheldon.send('conversation1', 'hola')

    #     messages_to_sheldon = sheldon.get_all()
    #     unread_to_queue = self.server.get_all('unread', retry=True)

    #     self.assertEqual(len(messages_to_sheldon), 1)
    #     self.assertEqual(len(unread_to_queue), 1)

    def test_basic_receive_context_activity(self):
        """
        Given two users in a context
        When a message is posted to the context
        Then each of them receives both messages
        """
        self.server.create_users(['sheldon', 'leonard'])
        self.server.activity.create('context1', users=['sheldon', 'leonard'])

        sheldon = self.getClient('sheldon')
        leonard = self.getClient('leonard')

        self.server.send('activity', 'Hello!', 'context1')
        messages_to_sheldon = sheldon.get_all(retry=True)
        messages_to_leonard = leonard.get_all(retry=True)

        self.assertEqual(len(messages_to_sheldon), 1)
        self.assertEqual(len(messages_to_leonard), 1)
