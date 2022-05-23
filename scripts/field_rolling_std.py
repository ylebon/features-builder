from dataclasses import dataclass

from copy import copy
from hamcrest import assert_that, instance_of

from indicators.core.candle.candle_element import CandleElement
from indicators.core.candle.candle_rolling import CandleRolling


@dataclass()
class FeatureScript:
    """
    Feature script: Rolling Standard Deviation

    """
    window: int
    resample: int = None
    diff: bool = False

    def __post_init__(self):
        """
        Post init

        """
        assert_that(self.window, instance_of(str))
        self.candle = CandleRolling.from_empty(self.window_int)

    @property
    def window_int(self):
        """
        Window integer

        """
        return int(self.window.replace("s", ""))

    def execute(self, dt, value):
        """
        Execute Profit

        """
        candle_element = CandleElement(dt, value)
        self.candle.update(candle_element)
        if not self.candle.full:
            return None
        else:
            std = self.candle.container.get_std()
            return std

    def create_dataset(self, name, df):
        """
        Create DataSet

        """
        field = df.columns[0]
        df[name] = df[field].rolling(self.window, min_periods=self.window_int).std()
        return df[[name]]
