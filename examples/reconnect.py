#!usr/bin/env python
# -*- coding:utf-8 -*-

"""
@author:max
@file: reconnect.py
@time: 2019/03/01
"""

import argparse
import collections
import logging
from kiel.clients.reconnect import ReconnectingClient
from tornado import gen, ioloop

from kiel.clients import SingleConsumer

log = logging.getLogger()

parser = argparse.ArgumentParser(
    description="Example script that consumes messages of a given topic."
)
parser.add_argument(
    "--brokers", type=lambda v: v.split(","), default="127.0.0.1:9092",
    help="Comma-separated list of bootstrap broker servers"
)
parser.add_argument(
    "--topic", type=str, default="test",
    help="Topic to consume"
)
parser.add_argument(
    "--status_interval", type=int, default=5,
    help="Interval (in seconds) to print the current status."
)
parser.add_argument(
    "--debug", type=bool, default=False,
    help="Sets the logging level to DEBUG"
)

color_counter = collections.Counter()


def run(args):
    @gen.coroutine
    def _run(c):
        while True:
            msgs = yield c.consume(args.topic)

            color_counter.update([msg["color"] for msg in msgs])

    return _run


def show_status():
    print (
            "counts: \n%s" % "\n".join([
        "\t%s: %s" % (color, count)
        for color, count in color_counter.most_common()
    ])
    )


def main():
    args = parser.parse_args()
    loop = ioloop.IOLoop.instance()

    if args.debug:
        log.setLevel(logging.DEBUG)

    consumer = ReconnectingClient(SingleConsumer(brokers=args.brokers), 'consumer')

    loop.spawn_callback(consumer.get_callback(run(args)))

    # loop.add_callback(run, consumer)
    status_callback = ioloop.PeriodicCallback(
        show_status, args.status_interval * 1000
    )

    def wind_down(_):
        status_callback.stop()
        loop.stop()

    try:
        status_callback.start()
        loop.start()
    except KeyboardInterrupt:
        consumer.close().add_done_callback(wind_down)


if __name__ == "__main__":
    main()
