# -*- coding: utf-8 -*-

import os
import ssl
import logging
import configparser
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy

logger = logging.getLogger(__name__)

# load REDIS settings
config = configparser.ConfigParser()
config.read(os.getenv('SETTINGS_FILE', './settings.ini'))

if 'CASSANDRA' not in config:
    raise ValueError('No [CASSANDRA] section inside the settings file')

settings = config['CASSANDRA']
cassandra_contact_points = settings.get('CONTACT_POINTS', fallback='localhost').split(',')
cassandra_port = settings.getint('PORT', fallback=9042)

cassandra_username = settings.get('USERNAME', fallback=None)
cassandra_password = settings.get('PASSWORD', fallback=None)

cassandra_local_datacenter = settings.get('LOCAL_DATACENTER', fallback=None)
cassandra_keyspace = settings.get('KEYSPACE', fallback=None)

# prepare ssl
cassandra_use_ssl = settings.getboolean('USE_SSL', fallback=False)
cassandra_ssl_cert_file = settings.get('SSL_CERT_FILE', fallback=None)
cassandra_ssl_key_file = settings.get('SSL_KEY_FILE', fallback=None)
cassandra_ssl_password = settings.get('SSL_PASSWORD', fallback=None)
cassandra_ssl = None
if cassandra_use_ssl:
    cassandra_ssl = ssl.SSLContext(ssl.PROTOCOL_TLS)
    cassandra_ssl.load_cert_chain(
        certfile=cassandra_ssl_cert_file,
        keyfile=cassandra_ssl_key_file,
        password=cassandra_ssl_password
    )


class Client():

    cassandra_connection = None

    @classmethod
    def open_connection(cls):
        if cls.cassandra_connection:
            # already connected
            return

        auth_provider = PlainTextAuthProvider(
            username=cassandra_username,
            password=cassandra_password
        )

        cls.cassandra_connection = Cluster(
            cassandra_contact_points,
            port=cassandra_port,
            auth_provider=auth_provider,
            ssl_context=cassandra_ssl,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=cassandra_local_datacenter)
        )

    @classmethod
    def get_client(cls):
        if not cls.cassandra_connection:
            cls.open_connection()
        return cls.cassandra_connection.connect(cassandra_keyspace)

    @classmethod
    def close_connection(cls):
        if cls.cassandra_connection:
            return
        cls.cassandra_connection.shutdown()
        cls.cassandra_connection = None
