from dataclasses import dataclass

from datetime import datetime
from hamcrest import *

from indicators.core.candle.candle_element import CandleElement


@dataclass
class FeatureScript:
    """
    Feature script: Rolling Covariance

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

        dt = datetime.fromtimestamp(ts)
        candle_element = CandleElement(dt, value)
        self.candle.update(candle_element)
        return self.candle.container.get_mean()

    def create_dataset(self, name, df):
        """
        Create DataSet

        """
        spread = df['ask_price'] - df['bid_price']
        df[name] = spread.rolling(self.window, min_periods=self.window_int).cov()
        return df[[name]]
