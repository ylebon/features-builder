from dataclasses import dataclass

import numpy as np
from datetime import datetime
from hamcrest import *

from indicators.core.candle.candle_element import CandleElement


@dataclass
class FeatureScript:
    """
    Feature script: Rolling Diff Quantile 50

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
        field = df.columns[0]
        rolling = df[field].rolling(self.window, min_periods=self.window_int)
        df[name] = rolling.quantile(0.5) - rolling.quantile(0.0)
        return df[[name]]
