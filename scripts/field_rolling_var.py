from dataclasses import dataclass
import pandas as pd
from hamcrest import assert_that, instance_of
from copy import copy
from indicators.core.candle.candle_element import CandleElement
from indicators.core.candle.candle_rolling import CandleRolling


@dataclass()
class FeatureScript:
    """
    Feature script: Rolling Var

    """
    window: int
    resample: int = None

    @property
    def window_int(self):
        """
        Window integer

        """
        return int(self.window.replace("s", ""))

    def __post_init__(self):
        """
        Post init

        """
        assert_that(self.window, instance_of(str))
        self.candle = CandleRolling.from_empty(self.window_int)

    def execute(self, dt, value):
        """
        Execute Profit

        """
        candle_element = CandleElement(dt, value)
        self.candle.update(candle_element)
        if not self.candle.full:
            return None
        else:
            var = self.candle.container.get_var()
            return var

    def create_dataset(self, name, df):
        """
        Create DataSet

        """
        field = df.columns[0]
        df[name] = df[field].rolling(self.window, min_periods=self.window_int).var()
        return df[[name]]
