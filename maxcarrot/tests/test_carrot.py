from maxcarrot.tests import RabbitTests


class FunctionalTests(RabbitTests):
    # BEGIN TESTS

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
