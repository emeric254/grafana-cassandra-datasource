#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import signal
import logging
import configparser
from tornado import web
from handlers.QueryHandler import QueryHandler
from handlers.HealthHandler import HealthHandler
from handlers.VersionHandler import VersionHandler
from tools import server
from tools.CassandraClient import Client

abs_path = os.path.abspath(__file__)
d_name = os.path.dirname(abs_path)
os.chdir(d_name)

# app's title
__title__ = 'Cassandra gateway'

# load GENERAL settings
config = configparser.ConfigParser()
config.read(os.getenv('SETTINGS_FILE', './settings.ini'))

if 'GENERAL' not in config:
    raise ValueError('No [GENERAL] section inside the settings file')

settings = config['GENERAL']
logfile = settings.get('LOG_FILE', fallback=None)
verbosity = settings.get('LOG_VERBOSITY', fallback='INFO')

log_format = '%(asctime)s - %(name)s [%(levelname)s] %(message)s'
logging.basicConfig(filename=logfile, level=verbosity, format=log_format)
logger = logging.getLogger(__name__)


def main():
    """
    main method.

    :return:
    """
    app_settings = {}

    application = web.Application([
            (r'/version', VersionHandler),
            (r'/health', HealthHandler),
            (r'/query', QueryHandler),
        ], **app_settings)

    server.start_http(app=application)
    logger.info('Started, ready to serve')


def stop(*_):
    logger.info("Stopping")
    Client.close_connection()
    server.stop_all()
    exit(0)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    main()

