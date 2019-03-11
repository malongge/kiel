#!usr/bin/env python
# -*- coding:utf-8 -*-

"""
@author:max
@file: reconnect.py
@time: 2019/03/01
"""

import logging

from tornado import gen
from kiel.exc import NoBrokersError

logger = logging.getLogger(__file__)


class ReconnectingClient(object):
    """
    Wraps an client such that it will reconnect forever.
    Provides a way to add worker functions to use the wrapped client forever.
    example:
        client = ReconnectingClient(clients.SingleConsumer(brokers=["localhost"]), "consumer")
        loop.create_task(client.run(my_consumer_func))
    """

    def __init__(self, client, name, retry_interval=5):

        self.client = client
        self.retry_interval = retry_interval
        self.connected = False
        self.name = name

    @gen.coroutine
    def start(self):
        """
        Connects the wrapped client.
        """
        while not self.connected:
            try:
                logger.info("Attempting to connect %s client.", self.name)
                yield self.client.connect()
                logger.info("%s client connected successfully.", self.name)
                self.connected = True
            except NoBrokersError:
                logger.exception(
                    "Failed to connect %s client, retrying in %d seconds.",
                    self.name,
                    self.retry_interval,
                )
                yield gen.sleep(self.retry_interval)

    @gen.coroutine
    def close(self):
        """
        close wrapped client.
        :return:
        """
        if not self.connected:
            pass
        try:
            logger.info("close connect %s client", self.name)
            yield self.client.close()
        except Exception as e:
            logger.exception(str(e))

    @gen.coroutine
    def work(self, worker):
        """
        Executes the worker function.
        """
        try:
            yield worker(self.client)
        except NoBrokersError:
            logger.exception(
                "Encountered exception while working %s client, reconnecting.",
                self.name,
            )
            self.connected = False

    def get_callback(self, worker, cond=lambda v: True):
        """
        Returns a callback function that will ensure that the wrapped client is
        connected forever.
        example:
            loop.spawn_callback(client.get_callback(my_cb))
        """

        @gen.coroutine
        def _f():
            v = cond(None)
            while cond(v):
                yield self.start()
                v = yield self.work(worker)

        return _f

    def run(self, worker, cond=lambda v: True):
        """
        Returns a coroutine that will ensure that the wrapped client is
        connected forever.
        example:
            loop.create_task(client.run(my_func))
        """
        return self.get_callback(worker, cond)()
