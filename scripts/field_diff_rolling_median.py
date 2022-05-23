from copy import copy
from dataclasses import dataclass

import numpy as np
from logbook import Logger

from indicators.core.candle.candle_element import CandleElement
from indicators.core.candle.candle_rolling import CandleRolling


@dataclass
class FeatureScript:
    """
    Rolling Median Diff

    """
    window: str
    resample: str = None

    def __post_init__(self):
        """
        Post init

        """
        self.log = Logger("feature - rolling mean")
        self.candle = CandleRolling.from_empty(self.window_int)
        self.log.info(f"window='{self.window_int}'")

    @property
    def window_int(self):
        """
        Window integer

        """
        return int(self.window.replace("s", ""))

    def execute(self, dt, value):
        """
        Execute

        """
        candle_element = CandleElement(dt, value)
        self.candle.update(candle_element)
        if not self.candle.full:
            return None
        else:
            return self.candle.container.get_mean()

    def create_dataset(self, name, df):
        """
        Create DataSet

        """
        field = df.columns[0]
        rolling = df[field].diff().rolling(self.window, min_periods=self.window_int)
        df[name] = rolling.median()
        return df[[name]]
