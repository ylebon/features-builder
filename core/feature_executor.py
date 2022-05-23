import asyncio
import os
import time
from datetime import timedelta
from multiprocessing import Pool, cpu_count

import pandas as pd
from dask import dataframe as dd
from dask import delayed
from logbook import Logger
from pydantic import BaseModel

from events import Events
from events.feature.feature import Feature
from features.core.feature_builder import FeatureBuilder
from features.core.feature_loader import FeatureLoader
from tasks.core.tasks.features import dataframe_to_s3
from tasks.core.tasks.feeds import load_parquet
from tasks.core.tasks.parquet import sync_s3

pd.set_option('mode.chained_assignment', None)


def feature_builder_worker(feature_name, feature_data, instrument_id, exchange, symbol, date):
    """
    Feature builder worker

    """
    fields = ["value", 'feature', 'exchange', 'symbol', 'date']
    print(f"msg='creating feature' name='{feature_name}'")

    # feature builder
    feature_builder = FeatureBuilder.from_data(feature_data)

    print(f"msg='creating feature' name='{feature_name}' columns='{feature_builder.columns}'")

    # load parquet
    t_load_parquet = load_parquet.Task("load_parquet")
    df = t_load_parquet.run(instrument_id, columns=feature_builder.columns, date=date)

    # resample 1S and backfill
    df = df.resample("1S").bfill()

    # create feature builder
    ddf = dd.from_pandas(df, npartitions=1)
    dataset = feature_builder.create_dataset(feature_name, ddf).compute()
    dataset = dataset.rename(columns={feature_name: "value"})
    dataset['exchange'] = exchange
    dataset['symbol'] = symbol
    dataset['feature'] = feature_name
    dataset['date'] = dataset.index.strftime('%Y%m%d')
    dataset = dd.from_pandas(dataset[fields], npartitions=1)

    print(f"msg='feature created' name='{feature_name}'")
    t_features_to_s3 = dataframe_to_s3.Task("features_to_s3")
    t_features_to_s3.run(dataset)

    # clean
    del df
    del dataset
    return feature_name


@delayed
def dump_to_s3(feature_name, exchange, symbol, dataset):
    """
    Dump data to S3

    """
    print(f"msg='feature created' feature='{feature_name}' length='{len(dataset.index)}''")
    fields = ["value", 'feature', 'exchange', 'symbol', 'date']
    dataset = dataset.rename(columns={feature_name: "value"})
    dataset['exchange'] = exchange
    dataset['symbol'] = symbol
    dataset['feature'] = feature_name
    print(dataset.index)
    dataset['date'] = dataset.index.strftime('%Y%m%d')
    dataset = dd.from_pandas(dataset[fields], npartitions=1)
    t_features_to_s3 = dataframe_to_s3.Task("features_to_s3")
    t_features_to_s3.run(dataset)
    return dataset


@delayed
def print_df_count(df):
    """
    Cound columns in df

    """
    print(df.count())


class FeatureExecutor(BaseModel):
    """
    Feature executor

    """
    features_builders: dict = None
    feature_loader: FeatureLoader
    queue: asyncio.Queue = None
    log: Logger = None
    handlers: dict = None
    max_size: int = 0
    features_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'features'))

    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def get_features_dir(cls):
        features_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'features'))
        return features_dir

    @classmethod
    def create(cls, queue=None):
        """
        From data

        """
        kwargs = dict()
        features_dir = cls.get_features_dir()
        kwargs['feature_loader'] = FeatureLoader.from_file(features_dir)
        kwargs['queue'] = queue
        kwargs['log'] = Logger("FeatureExecutor")
        kwargs['handlers'] = dict()
        feature_executor = FeatureExecutor(**kwargs)
        feature_executor._load_builders()
        return feature_executor

    def _load_builders(self):
        """
        Load builders

        """
        self.features_builders = self.feature_loader.builder_list

    async def run(self, features=None, max_size=7300, min_size=3600, loop=None):
        """
        Start executor

        """
        self.log.info(f"msg='start features executor' min_size='{min_size}' max_size='{max_size}'")
        features = FeatureLoader.filter_features(self.features_dir, features)
        self.log.info(f"msg='number of features' features='{len(features)}'")

        data = dict()

        @delayed
        def compute_feature(feature, df):
            feature_value = self.features_builders[feature].create_dataset(feature, df).iloc[-1][feature]
            return feature, feature_value

        def process_df(df):
            """
            Process Dataframe

            """
            start_time = time.time()

            ddf = dd.from_pandas(df, npartitions=1)
            ddf = ddf.persist()

            # set operations
            futures = [compute_feature(feature, ddf) for feature in features]

            # compute result
            result = dd.compute(*futures)

            # duration
            duration = time.time() - start_time

            # feature
            for name, value in result:
                feature = Feature(**{
                    'exchange': event.exchange,
                    'seq': event.seq,
                    'symbol': event.symbol,
                    'name': name,
                    'value': value,
                    'meta': {
                        'duration': duration
                    }
                })

                for handler in self.handlers.get(Events.FEATURE, []):
                    handler(feature)

        while True:
            event = await self.queue.get()

            self.log.debug(
                f"msg='features building started' exchange='{event.exchange}' symbol='{event.symbol}' seq='{event.seq}'")

            # bid price
            instrument_id = f'{event.exchange.upper()}_{event.symbol}'
            index = pd.to_datetime(event.marketdata_timestamp, unit='s')
            try:
                data[instrument_id][index] = {'bid_price': event.bid_price, 'ask_price': event.ask_price,
                                              'bid_qty': event.bid_qty, 'ask_qty': event.ask_qty, 'date': event.date}
            except KeyError:
                data[instrument_id] = dict()
                data[instrument_id][index] = {'bid_price': event.bid_price, 'ask_price': event.ask_price,
                                              'bid_qty': event.bid_qty, 'ask_qty': event.ask_qty, 'date': event.date}

            # create dataframe
            df = pd.DataFrame.from_dict(data[instrument_id], orient='index')
            df = df.resample("1S").bfill()
            df_end_date = df.index[-1]
            df_start_date = df.index[0]
            time_range = df_end_date - df_start_date

            if timedelta(seconds=max_size) < time_range:
                t1 = time.time()
                self.log.info(f"msg='cleaning data' instrument='{instrument_id}'")
                df = df[df.index > (df.index[-1] - timedelta(seconds=min_size))]
                data[instrument_id] = df.to_dict(orient='index')
                self.log.info(
                    f"msg='data has been cleaned' instrument_id='{instrument_id}' duration='{time.time()-t1}' start_date='{df.index[0]}' end_date='{df.index[-1]}'"
                )
            await loop.run_in_executor(None, process_df, df)

    def create_dataset(self, instrument_id, features, date=None):
        """
        Create dataset

        """
        # features
        features = features or list(self.features_builders.keys())
        features = FeatureLoader.filter_features(self.features_dir, features)
        self.log.info(f"msg='creating features dataset' features='{len(features)}'")

        # sync feeds parquet
        exchange, symbol = instrument_id.split("_", 1)
        parquet_filter = {"exchange": exchange, "symbol": symbol}
        t_sync_s3 = sync_s3.Task("sync_s3")
        t_sync_s3.run(parquet_filter=parquet_filter)

        # delayed func list
        start_time = time.time()

        # number of cpu count
        if os.getenv("MAX_PROCS", None):
            max_procs = int(os.getenv("MAX_PROCS"))
        else:
            max_procs = cpu_count() - 1

        p = Pool(max_procs, maxtasksperchild=1)
        p.starmap(feature_builder_worker,
                  [(f, FeatureLoader.load_data(self.features_dir, f), instrument_id, exchange, symbol, date)
                   for f
                   in features])

        duration = time.time() - start_time
        self.log.info(f"msg='features dataset created' duration='{duration}'")

    def on_event(self, event_type):
        """
        Register event handlers

        """

        def register_handler(handler):
            try:
                self.handlers[event_type].append(handler)
            except KeyError:
                self.handlers[event_type] = [handler]
            return handler

        return register_handler


if __name__ == "__main__":
    from logbook import StreamHandler
    import sys

    StreamHandler(sys.stdout).push_application()
    instrument_id = "BINANCE_BTC_USDT"
    features = ['@spread_rolling_mean', '@spread_rolling_q50']
    feature_executor = FeatureExecutor.create()
    feature_executor.create_dataset(instrument_id, features, date="*")
