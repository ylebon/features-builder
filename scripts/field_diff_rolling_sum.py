from dataclasses import dataclass

from hamcrest import *
import numpy as np

@dataclass
class FeatureScript:
    """
    Feature script: Rolling CumSum diff

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
        Execute rolling

        """
        pass

    def create_dataset(self, name, df):
        """
        Create DataSet

        """
        field = df.columns[0]
        rolling = df[field].diff().rolling(self.window, min_periods=self.window_int)
        df[name] = rolling.sum()
        return df[[name]]

