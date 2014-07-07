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

        sheldon.conversations.send('conversation1', 'Hello!')
        leonard.conversations.send('conversation1', 'Hello...')

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

        sheldon.send_internal('Hello!')

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

        sheldon.conversations.send('conversation1', 'Hello!')
        leonard.conversations.send('conversation1', 'Hello...')

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
        And thte push queue doesn't receive the message
        And the user also receives the message
        """
        self.server.create_users(['sheldon', 'leonard'])
        self.server.conversations.create('conversation1', users=['sheldon', 'leonard'])

        sheldon = self.getClient('sheldon')
        sheldon.conversations.send('conversation1', 'Hello!')
        messages_to_sheldon = sheldon.get_all()
        messages_to_messages_queue = self.server.get_all('messages')
        messages_to_push_queue = self.server.get_all('push')

        self.assertEqual(len(messages_to_sheldon), 1)
        self.assertEqual(len(messages_to_messages_queue), 1)
        self.assertEqual(len(messages_to_push_queue), 0)

    def test_messages_queue_receive_other_messages(self):
        """
        Given a conversation
        When a message is posted
        And this message is not a chat message
        Then the push queue receives the message
        And and the messages queue don't
        And the user also receives the message
        """
        self.server.create_users(['sheldon', 'leonard'])
        self.server.conversations.create('conversation1', users=['sheldon', 'leonard'])

        sheldon = self.getClient('sheldon')
        leonard = self.getClient('leonard')
        sheldon.conversations.send('conversation1', 'Hello!', destination='notifications')
        messages_to_sheldon = sheldon.get_all()
        messages_to_leonard = leonard.get_all()
        messages_to_messages_queue = self.server.get_all('messages')
        messages_to_push_queue = self.server.get_all('push')

        self.assertEqual(len(messages_to_sheldon), 1)
        self.assertEqual(len(messages_to_leonard), 1)
        self.assertEqual(len(messages_to_messages_queue), 0)
        self.assertEqual(len(messages_to_push_queue), 1)

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

        messages_to_sheldon = sheldon.get_all()
        messages_to_penny = penny.get_all()
        messages_to_messages_queue = self.server.get_all('messages')
        messages_to_push_queue = self.server.get_all('push')

        self.assertEqual(len(messages_to_sheldon), 1)
        self.assertEqual(len(messages_to_penny), 1)
        self.assertEqual(len(messages_to_messages_queue), 2)
        self.assertEqual(len(messages_to_push_queue), 0)

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

        messages_to_sheldon = sheldon.get_all()
        messages_to_penny = penny.get_all()
        messages_to_messages_queue = self.server.get_all('messages')
        messages_to_push_queue = self.server.get_all('push')

        self.assertEqual(len(messages_to_sheldon), 0)
        self.assertEqual(len(messages_to_penny), 0)
        self.assertEqual(len(messages_to_messages_queue), 0)
        self.assertEqual(len(messages_to_push_queue), 0)

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

        self.server.management.load_exchanges()
        self.assertIn('sheldon.publish', self.server.management.exchanges_by_name)
        self.assertIn('sheldon.subscribe', self.server.management.exchanges_by_name)

    def test_delete_user(self):
        """
        Given two users in a conversation
        When one of them gets removed from the system
        Then it's associated exchanges disappear
        """
        self.server.create_users(['sheldon', 'leonard'])
        self.server.conversations.create('conversation1', users=['sheldon', 'leonard'])

        self.server.delete_user('sheldon')

        self.server.management.load_exchanges()
        self.assertNotIn('sheldon.publish', self.server.management.exchanges_by_name)
        self.assertNotIn('sheldon.subscribe', self.server.management.exchanges_by_name)

    def test_delete_conversation(self):
        """
        Given two users in a conversation
        When the conversation gets removed from the system
        Then it's associated bindings disappear
        """
        self.server.create_users(['sheldon', 'leonard'])
        self.server.conversations.create('conversation1', users=['sheldon', 'leonard'])

        self.server.conversations.delete('conversation1')

        bindings = self.server.management.load_exchange_bindings('conversations')
        conversation1_bindings = [binding for binding in bindings if binding['routing_key'].startswith('conversation1')]
        self.assertEqual(len(conversation1_bindings), 0)

    def test_delete_conversation_others_remain(self):
        """
        Given two users in a two conversations
        When a conversation gets removed from the system
        Then it's associated bindings disappear
        And the other conversation remains
        """
        self.server.create_users(['sheldon', 'leonard'])
        self.server.conversations.create('conversation1', users=['sheldon', 'leonard'])
        self.server.conversations.create('conversation2', users=['sheldon', 'leonard'])

        self.server.conversations.delete('conversation1')

        bindings = self.server.management.load_exchange_bindings('conversations')

        conversation_bindings = [binding for binding in bindings if binding['routing_key'].startswith('conversation')]
        self.assertEqual(len(conversation_bindings), 4)

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
        messages_to_push_queue = self.server.get_all('push')

        self.assertEqual(len(messages_to_sheldon), 1)
        self.assertEqual(len(messages_to_leonard), 1)
        self.assertEqual(len(messages_to_push_queue), 1)

    def test_basic_receive_multiple_context_activity(self):
        """
        Given three users in contexts
        When a message is posted to the contexts
        Then each of them receives only the messages from that context
        """
        self.server.create_users(['sheldon', 'leonard', 'penny'])
        self.server.activity.create('context1', users=['sheldon', 'leonard'])
        self.server.activity.create('context2', users=['leonard', 'penny'])

        sheldon = self.getClient('sheldon')
        leonard = self.getClient('leonard')
        penny = self.getClient('penny')

        self.server.send('activity', 'Hello!', 'context1')
        self.server.send('activity', 'Hello!', 'context2')

        messages_to_sheldon = sheldon.get_all(retry=True)
        messages_to_leonard = leonard.get_all(retry=True)
        messages_to_penny = penny.get_all(retry=True)

        self.assertEqual(len(messages_to_sheldon), 1)
        self.assertEqual(len(messages_to_leonard), 2)
        self.assertEqual(len(messages_to_penny), 1)

    def test_basic_drop_context_activity(self):
        """
        Given two users in a context
        When a message is posted to the context
        Then the message don't reach other users
        """
        self.server.create_users(['sheldon', 'leonard', 'penny'])
        self.server.activity.create('context1', users=['sheldon', 'leonard'])

        sheldon = self.getClient('sheldon')
        leonard = self.getClient('leonard')
        penny = self.getClient('penny')

        self.server.send('activity', 'Hello!', 'context1')
        messages_to_sheldon = sheldon.get_all(retry=True)
        messages_to_leonard = leonard.get_all(retry=True)
        messages_to_penny = penny.get_all()

        self.assertEqual(len(messages_to_sheldon), 1)
        self.assertEqual(len(messages_to_leonard), 1)
        self.assertEqual(len(messages_to_penny), 0)

    def test_delete_context(self):
        """
        Given two users in a conversation
        When the conversation gets removed from the system
        Then it's associated bindings disappear
        """
        self.server.create_users(['sheldon', 'leonard'])
        self.server.activity.create('context1', users=['sheldon', 'leonard'])

        self.server.activity.delete('context1')

        bindings = self.server.management.load_exchange_bindings('activity')
        context_bindings = [binding for binding in bindings if binding['routing_key'].startswith('context1')]
        self.assertEqual(len(context_bindings), 0)

    def test_delete_context_others_remain(self):
        """
        Given two users in a two conversations
        When a conversation gets removed from the system
        Then it's associated bindings disappear
        And the other conversation remains
        """
        self.server.create_users(['sheldon', 'leonard'])
        self.server.activity.create('context1', users=['sheldon', 'leonard'])
        self.server.activity.create('context2', users=['sheldon', 'leonard'])

        self.server.activity.delete('context1')

        bindings = self.server.management.load_exchange_bindings('activity')

        context_bindings = [binding for binding in bindings if binding['routing_key'].startswith('context')]
        self.assertEqual(len(context_bindings), 2)
