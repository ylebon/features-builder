# -*- coding: utf-8 -*-

usage = """Signaler.

Usage:
  features_publisher.py [--router=<router>] [--feed=<feed>...] [--loglevel=<loglevel>] [--feature=<feature>]
  features_publisher.py (-h | --help)
  features_publisher.py --version
Options:
  -r --router=<router>          Set router [default: ROUTER.OANDA]
  -f --feed=<feed>              Set feed [default: OANDA_EUR_USD]
  -t --feature=<feature>        Set feature
  -l --loglevel=<loglevel>      Set log level [default: INFO]
  -v --version                  Show version.
"""
import os
import sys

lib_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(lib_dir)
from docopt import docopt
from logbook import StreamHandler, set_datetime_format, FileHandler, NestedSetup
from config.core.config_services import ConfigServices
from patterns.recursionlimit import recursionlimit

from events import Events
from features.core.feature_executor import FeatureExecutor
from connector.core.connector_listener import ConnectorListener
import asyncio
from logbook import Logger


def main(arguments):
    # features publisher
    log = Logger("features_publisher")

    # setting services
    services_config = ConfigServices.create()

    # set router
    if not arguments['--router'].startswith("ws://"):
        router = services_config.get_value(arguments['--router'])
    else:
        router = arguments['--router']

    if router is None:
        print("Router address '{}' not correct!".format(arguments['--router']))
        sys.exit(-1)

    services_config.set_value('ROUTER.URL', router)

    print("  =============== PARAMETERS ===============   ")
    print("  - Router address   : {}".format(arguments['--router']))
    print("  ==========================================   ")

    # get asyncio loop
    loop = asyncio.get_event_loop()

    # queuek loop
    queue = asyncio.Queue(loop=loop)

    # feature list
    if arguments['--feature']:
        log.info("msg='reading features from command line'")
        features = arguments['--feature'].split(",")
    elif os.getenv("FEATURES", None):
        log.info("msg='reading features from environment'")
        features = os.getenv("FEATURES").split(",")
    else:
        features = None

    with recursionlimit(15000):

        # router connector
        feed_connector = ConnectorListener.from_router(router)

        # count
        # register market BBO
        for feed in arguments['--feed']:
            exchange, symbol = feed.lower().split("_", 1)

            @feed_connector.on_event(Events.MARKET_BBO, params={'exchange': exchange, 'symbol': symbol})
            async def fn_execute(event):
                log.debug(f"msg='adding event to queue' event='{event}' exchange='{exchange}' symbol='{symbol}'")
                await queue.put(event)

        # features connector
        features_connector = ConnectorListener.from_router(router)

        # feature executor
        feature_executor = FeatureExecutor.create(queue=queue)

        @feature_executor.on_event(Events.FEATURE)
        def publish_feature(feature):
            features_connector.publish(feature)

        # run features connectors
        loop.run_until_complete(loop.create_task(
            features_connector.run()
        ))

        # run features connectors
        loop.run_until_complete(loop.create_task(
            feed_connector.run()
        ))

        # run executor
        min_size = int(os.getenv("MIN_SIZE", 3600))
        max_size = int(os.getenv("MAX_SIZE", 7200))
        loop.run_until_complete(loop.create_task(
            feature_executor.run(min_size=min_size, max_size=max_size, features=features, loop=loop)
        ))

        # loop forever
        loop.run_forever()


if __name__ == '__main__':
    # parse arguments
    arguments = docopt(usage, version='0.0.1')
    set_datetime_format("local")

    # subscribe to instruments
    format_string = (
        '[{record.time:%Y-%m-%d %H:%M:%S.%f%z}] '
        '{record.level_name: <5} - {record.channel: <15}: {record.message}'
    )

    # log file
    log_file = os.path.join(os.path.dirname(__file__), '..', '..', 'logs', 'features.log')

    # create directory
    logs_dir = os.path.dirname(log_file)
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    # log level
    log_level = os.getenv("LOG_LEVEL", arguments['--loglevel'])

    # handlers
    handlers = [
        StreamHandler(sys.stdout, level=log_level, format_string=format_string, bubble=True),
        FileHandler(log_file, level='INFO', format_string=format_string, bubble=True),
    ]

    # log handlers
    with NestedSetup(handlers):
        main(arguments)
