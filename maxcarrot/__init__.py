import rabbitpy
import json


class RabbitWrapper(object):
    def __init__(self, url, username, password, bind=True):
        self.url = 'amqp://{}:{}@{}'.format(username, password, url)
        self.connection = rabbitpy.Connection(self.url)
        self.channel = self.connection.channel()

    def get_user_publish_exchange(self, username):
        # Exchange to sent messages to rabbit
        # routed by topic to destination
        return rabbitpy.Exchange(self.channel, '{}.publish'.format(username), exchange_type='direct')

    def get_user_subscribe_exchange(self, username):
        # Exchange to broadcast messages for this user to all
        # the consumers identified with this username

        return rabbitpy.Exchange(self.channel, '{}.subscribe'.format(username), exchange_type='fanout')

    def disconnect(self):
        self.connection.close()


class RabbitServer(RabbitWrapper):
    """
    """
    def __init__(self, url, username, password, bind=True):
        super(RabbitServer, self).__init__(url, username, password)

        self.exchanges = {}
        self.queues = {}
        # Defne global conversations exchange
        self.exchanges['conversations'] = rabbitpy.Exchange(self.channel, 'conversations', exchange_type='topic')

        # Define persistent queue for writing messages to max
        self.queues['messages'] = rabbitpy.Queue(self.channel, 'messages', durable=True)

        # Wrapper to interact with conversations
        self.conversations = RabbitConversations(self)

        self.declare()

        # Define messages queue to conversations to receive messages from all conversations
        self.queues['messages'].bind(source=self.exchanges['conversations'], routing_key='*')

    def declare(self):
        for exchange_name, exchange in self.exchanges.items():
            exchange.declare()
        for queue_name, queue in self.queues.items():
            queue.declare()


class RabbitConversations(object):
    """
        Wrapper around conversations, to send and receive messages as a user
    """

    def __init__(self, wrapper):
        self.client = wrapper
        self.exchange = rabbitpy.Exchange(self.client.channel, 'conversations', exchange_type='topic')

    def send(self, conversation_id, message):
        str_message = message if isinstance(message, basestring) else json.dumps(message)
        message = rabbitpy.Message(self.client.channel, str_message)
        message.publish(self.client.publish, routing_key=conversation_id)

    def get_all(self, conversation_id):
        messages = []
        message_obj = True
        while message_obj is not None:
            message_obj = self.get(conversation_id)
            if message_obj is not None:
                try:
                    message = message_obj.json()
                except ValueError:
                    message = message_obj.body
                messages.append(message)
        return messages

    def get(self, conversation_id):
        return self.client.queue.get()

    def create(self, conversation, users):
        for user in users:
            self.bind_user(conversation, user)

    def bind_user(self, conversation, user):
        # Messages from user to specific conversation
        # A binding is made for each conversation, this way we limit ilegal posting to unsubscribed conversations
        user_publish_exchange = self.client.get_user_publish_exchange(user)
        user_publish_exchange.declare()
        self.exchange.bind(user_publish_exchange, routing_key=conversation)

        # Messages from conversation to user
        # A binding is made for each conversation, to receive only messages from subscribed conversations
        user_subscribe_exchange = self.client.get_user_subscribe_exchange(user)
        user_subscribe_exchange.declare()
        user_subscribe_exchange.bind(self.exchange, routing_key=conversation)


class RabbitClient(RabbitWrapper):
    def __init__(self, url, username, password, bind=True):
        super(RabbitClient, self).__init__(url, 'guest', 'guest')
        self.username = username

        self.subscribe = self.get_user_subscribe_exchange(self.username)
        self.publish = self.get_user_publish_exchange(self.username)

        self.queue = rabbitpy.Queue(self.channel, exclusive=True)
        self.queue.declare()
        self.queue.bind(self.subscribe)

        # Wrapper to interact with conversations
        self.conversations = RabbitConversations(self)
