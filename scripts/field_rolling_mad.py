from dataclasses import dataclass
import numpy as np
from hamcrest import assert_that, instance_of

from indicators.core.candle.candle_rolling import CandleRolling

@dataclass()
class FeatureScript:
    """
    Feature script: Rolling MAD

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
        pass

    def create_dataset(self, name, df):
        """
        Create DataSet

        """
        field = df.columns[0]
        rolling = df[field].rolling(self.window, min_periods=self.window_int)
        df[name] = rolling.apply(lambda x: np.fabs(x - x.mean()).mean(), raw=False)
        return df[[name]]
