from maxcarrot.tests import RabbitTests


class FunctionalTests(RabbitTests):
    # BEGIN TESTS

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
