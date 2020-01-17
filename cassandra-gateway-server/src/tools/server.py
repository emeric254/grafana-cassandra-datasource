# -*- coding: utf-8 -*-

import os
import logging
import configparser
from tornado import httpserver, ioloop, netutil, process, web

logger = logging.getLogger(__name__)

# load SERVER settings
config = configparser.ConfigParser()
config.read(os.getenv('SETTINGS_FILE', './settings.ini'))

if 'SERVER' not in config:
    raise ValueError('No [SERVER] section inside the settings file')

settings = config['SERVER']
port = settings.getint('HTTP_PORT', fallback=80)
fork = settings.getboolean('FORK', fallback=False)

servers = []


def start_http(app: web.Application, http_port: int = port, use_fork: bool = fork):
    """
    Create app instance(s) binding a port.

    :param app: the app to execute in server instances
    :param http_port: port to bind
    :param use_fork: fork or not to use more than one CPU (process)
    """
    http_socket = netutil.bind_sockets(http_port)  # HTTP socket
    if use_fork:
        try:  # try to create threads
            process.fork_processes(0)  # fork
        except KeyboardInterrupt:  # except KeyboardInterrupt to "properly" exit
            ioloop.IOLoop.current().stop()
            exit(0)
        except AttributeError:  # OS without fork() support ...
            logger.warning('Can not fork, continuing with only one thread ...')
            # do nothing and continue without multi-threading

    logger.info('Start an HTTP request handler on port : ' + str(http_port))
    server = httpserver.HTTPServer(app)
    server.add_sockets(http_socket)  # bind http port

    global servers
    servers.append((server, ioloop.IOLoop.current()))

    # try to stay forever alive to satisfy user's requests, except on KeyboardInterrupt to "properly" exit
    try:
        ioloop.IOLoop.current().start()
    except KeyboardInterrupt:
        server.close_all_connections()
        server.stop()
        ioloop.IOLoop.current().stop()


def stop_all():
    """
    Take care of stopping all server process properly
    """
    for server, loop in servers:
        server.close_all_connections()
        server.stop()
        loop.stop()
