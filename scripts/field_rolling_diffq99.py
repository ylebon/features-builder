from copy import copy
from dataclasses import dataclass

from hamcrest import assert_that, instance_of


@dataclass()
class FeatureScript:
    """
    Feature script: Distance Between Mean and Value

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
        """
        Window integer

        """
        return int(self.window.replace("s", ""))

    def execute(self, ts, value):
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
        df[name] = df[field] - rolling.quantile(0.99)
        return df[[name]]
