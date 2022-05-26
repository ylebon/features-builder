usage = """Features builder console.

Usage:
  features.py list
  features.py (-h | --help)
  features.py --version
Options:
  -l --loglevel=<loglevel>      Set log level [default: INFO]
  -v --version                  Show version.
"""
import os
import sys

lib_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(lib_dir)
from docopt import docopt
from logbook import StreamHandler, set_datetime_format, FileHandler, NestedSetup
from core.feature_loader import FeatureLoader


def main(arguments):
    features_dir = os.path.join(os.path.dirname(__file__), '..', 'features')
    if arguments["list"]:
        names = FeatureLoader.filter_features(
            features_dir,
            []
        )

        print(f"* number of the features {len(names)} *")
        for name in names:
            print(f"- {name}")


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
    log_file = os.path.join(os.path.dirname(__file__), '..', 'logs', 'features-builder.log')

    # create directory
    logs_dir = os.path.dirname(log_file)
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    # handlers
    handlers = [
        StreamHandler(sys.stdout, level=arguments['--loglevel'], format_string=format_string, bubble=True),
        FileHandler(log_file, level='INFO', format_string=format_string, bubble=True),
    ]

    # log handlers
    with NestedSetup(handlers):
        main(arguments)
