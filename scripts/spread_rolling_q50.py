from dataclasses import dataclass

from hamcrest import *


@dataclass
class FeatureScript:
    """
    Feature script: Rolling Spread Median

    """
    window: int
    resample: int = None

    def __post_init__(self):
        """
        Post init

        """
        assert_that(self.window, instance_of(str))

    @property
    def window_int(self):
        """Rolling it

        """
        return int(self.window.replace("s", ""))

    def execute(self, ts, value):
        """
        Execute rolling OHLC

        """

        pass

    def create_dataset(self, name, df):
        """
        Create DataSet

        """
        spread = df['ask_price'] - df['bid_price']
        df[name] = spread.rolling(self.window, min_periods=self.window_int).quantile(0.5)
        return df[[name]]
