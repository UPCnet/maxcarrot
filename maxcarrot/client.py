# -*- coding: utf-8 -*-
from haigha.connections.rabbit_connection import RabbitConnection
from haigha.message import Message
from maxcarrot.management import RabbitManagement

import json
import re


class RabbitClient(object):
    """
        Wraps features defined in the rabbit max architecture
        resources specs defines rules to check exchanges and queues
        in order to declare them or delete on cleanups

        * Internal queues definitions are marked as native, to avoid deletion
        * Exchanges and queues that need to be created always are marked as global

    """

    resource_specs = {
        'exchanges': [
            {'name': 'conversations', 'spec': 'conversations', 'type': 'topic'},
            {'name': 'twitter', 'spec': 'twitter', 'type': 'fanout'},
            {'name': 'activity', 'spec': 'activity', 'type': 'topic'},
            {'name': 'user_subscribe', 'spec': '.*?.subscribe', 'type': 'fanout', 'global': False},
            {'name': 'user_publish', 'spec': '.*?.publish', 'type': 'topic', 'global': False},
            {'name': 'default', 'spec': '^$', 'type': 'direct', 'native': True},
            {'name': 'internal', 'spec': 'amq\..*?', 'type': '.*', 'native': True},
        ],
        'queues': [
            {'name': 'dynamic', 'spec': 'amq\..*?', 'native': True},
            {'name': 'messages', 'spec': 'messages', 'bindings': [
                {'exchange': 'conversations', 'routing_key': '*.messages'}
            ]},
            {'name': 'push', 'spec': 'push', 'bindings': [
                {'exchange': 'conversations', 'routing_key': '*.notifications'},
                {'exchange': 'activity', 'routing_key': '#'}
            ]},
            {'name': 'twitter', 'spec': 'twitter', 'bindings': [
                {'exchange': 'twitter'}
            ]},
            {'name': 'tweety_restart', 'spec': 'tweety_restart'},
            {'name': 'twitter', 'spec': 'twitter'},
        ]
    }

    def __init__(self, url, declare=False, user=None):
        self.connect(url)

        if declare:
            self.declare()

        self.exchange_specs_by_name = {spec['name']: spec for spec in self.resource_specs['exchanges']}
        self.queue_specs_by_name = {spec['name']: spec for spec in self.resource_specs['queues']}

        # Wrapper to interact with conversations
        self.conversations = RabbitConversations(self)
        self.activity = RabbitActivity(self)
        self.management = RabbitManagement(self, 'http://{}:15672/api'.format(self.host), self.vhost_url, self.user, self.password)

        if user is not None:
            self.bind(user)

    def spec_by_name(self, type, name):
        for spec in self.resource_specs[type]:
            if spec['name'] == name:
                return spec
        return None

    def connect(self, url):
        """
            Connect to rabbitmq and create a channel
        """
        parts = re.search(r'amqp://(\w+):(\w+)@([^\:]+)\:(\d+)\/(.*)\/?', url).groups()
        self.user, self.password, self.host, self.port, self.vhost_url = parts
        self.vhost = self.vhost_url.replace('%2F', '/')
        self.connection = RabbitConnection(
            user=self.user, password=self.password,
            vhost=self.vhost, host=self.host,
            heartbeat=None, debug=True)
        self.ch = self.connection.channel()

    def disconnect(self):
        """
            Disconnecto from rabbitmq
        """
        self.connection.close()

    def declare(self):
        """
            Create all defined exchanges and queues on rabbit.
            Ignore those marked as internal or not global
        """
        for exchange in self.resource_specs['exchanges']:
            if exchange.get('global', True) and not exchange.get('native', False):
                self.ch.exchange.declare(
                    exchange=exchange['name'],
                    type=exchange['type'],
                    durable=True,
                    auto_delete=False
                )

        for queue in self.resource_specs['queues']:
            if queue.get('global', True) and not queue.get('native', False):
                self.ch.queue.declare(
                    queue=queue['name'],
                    durable=True,
                    auto_delete=False
                )
                for binding in queue.get('bindings', []):
                    self.ch.queue.bind(
                        queue=queue['name'],
                        exchange=binding['exchange'],
                        routing_key=binding.get('routing_key', ''),
                    )

    def bind(self, username):
        """
        Declare a dynamic queue to consume user messages
        and set active user
        """
        self.user = username
        self.queue, mc, cc = self.ch.queue.declare(exclusive=True)
        self.ch.queue.bind(self.queue, self.user_subscribe_exchange(username))

    def create_users(self, usernames):
        """
            Batch create user exchanges
        """
        for username in usernames:
            self.create_user(username)

    def create_user(self, username):
        """
            Creates user exchanges and internal binding
        """
        self.ch.exchange.declare(
            exchange=self.user_publish_exchange(username),
            type=self.exchange_specs_by_name['user_publish']['type'],
            durable=True,
            auto_delete=False
        )

        self.ch.exchange.declare(
            exchange=self.user_subscribe_exchange(username),
            type=self.exchange_specs_by_name['user_subscribe']['type'],
            durable=True,
            auto_delete=False
        )

        self.ch.exchange.bind(
            exchange=self.user_subscribe_exchange(username),
            source=self.user_publish_exchange(username),
            routing_key='internal',
        )

    def delete_user(self, username):
        self.ch.exchange.delete(self.user_publish_exchange(username))
        self.ch.exchange.delete(self.user_subscribe_exchange(username))

    def user_publish_exchange(self, username):
        """
            Returns name of exchange used to send messages to rabbit
        """
        return '{}.publish'.format(username)

    def user_subscribe_exchange(self, username):
        """
            Returns name of exchange used to broadcast messages for this user to all
            the consumers identified with this username
        """
        return '{}.subscribe'.format(username)

    def send(self, exchange, message, routing_key=''):
        body = message if isinstance(message, basestring) else json.dumps(message)
        message = Message(body)
        self.ch.publish(message, exchange, routing_key=routing_key)

    def send_internal(self, message):
        self.send(self.user_publish_exchange(self.user), message, routing_key='internal')

    def get_all(self, queue=None, retry=False):
        queue_name = self.queue if queue is None else queue
        messages = []
        message_obj = True
        tries = 1 if not retry else -1
        while not(tries == 0 or messages != []):
            while message_obj is not None:
                message_obj = self.get(queue_name)
                if message_obj is not None:
                    tries = 1
                    try:
                        message = (json.loads(str(message_obj.body)), message_obj)
                    except ValueError:
                        message = (message_obj.body, message_obj)
                    messages.append(message)
            tries -= 1
            message_obj = True
        return messages

    def get(self, queue_name):
        return self.ch.basic.get(queue_name)


class RabbitConversations(object):
    """
        Wrapper around conversations, to send and receive messages as a user
    """

    def __init__(self, client):
        self.client = client

    def send(self, conversation, message, destination='messages'):
        routing_key = '{}.{}'.format(conversation, destination)
        self.client.send(
            self.client.user_publish_exchange(self.client.user),
            message,
            routing_key=routing_key)

    def create(self, conversation, users):
        """
            Batch creates conversation bindings for a bunch
            of users
        """
        for user in users:
            self.bind_user(conversation, user)

    def bind_user(self, conversation, username):
        """
            Creates the bindings to route messages between
            a conversation and a user
        """
        self.client.ch.exchange.bind(
            exchange='conversations',
            source=self.client.user_publish_exchange(username),
            routing_key='{}.*'.format(conversation)
        )
        self.client.ch.exchange.bind(
            exchange=self.client.user_subscribe_exchange(username),
            source='conversations',
            routing_key='{}.*'.format(conversation)
        )

    def unbind_user(self, conversation, username):
        """
            Deletes the bindings to route messages between
            a conversation and a user
        """
        self.client.ch.exchange.unbind(
            exchange='conversations',
            source=self.client.user_publish_exchange(username),
            routing_key='{}.*'.format(conversation)
        )
        self.client.ch.exchange.unbind(
            exchange=self.client.user_subscribe_exchange(username),
            source='conversations',
            routing_key='{}.*'.format(conversation)
        )

    def delete(self, conversation):
        """
            Deletes all the bindings for a specific conversation
        """
        bindings = self.client.management.load_exchange_bindings('conversations')
        for binding in bindings:
            if binding['routing_key'].startswith(conversation):
                self.client.ch.exchange.unbind(
                    exchange=binding['destination'],
                    source=binding['source'],
                    routing_key=binding['routing_key']
                )


class RabbitActivity(object):
    """
        Wrapper around context activity, to receive messages as a user
    """

    def __init__(self, wrapper):
        self.client = wrapper

    def create(self, context, users):
        """
            Batch create user context bindings
            for a bunch of users
        """
        for user in users:
            self.bind_user(context, user)

    def bind_user(self, context, username):
        """
            Creates the binding to route messages from a context
            to a specific user
        """
        self.client.ch.exchange.bind(
            exchange=self.client.user_subscribe_exchange(username),
            source='activity',
            routing_key=context
        )

    def unbind_user(self, context, username):
        """
            Deletes the bindings to route messages from a context
            to a specific user
        """
        self.client.ch.exchange.unbind(
            exchange=self.client.user_subscribe_exchange(username),
            source='activity',
            routing_key=context
        )

    def delete(self, context):
        """
            Deletes all the bindings for a specific conversation
        """
        bindings = self.client.management.load_exchange_bindings('activity')
        for binding in bindings:
            if binding['routing_key'].startswith(context):
                self.client.ch.exchange.unbind(
                    exchange=binding['destination'],
                    source=binding['source'],
                    routing_key=binding['routing_key']
                )
