from dataclasses import dataclass

from hamcrest import assert_that, instance_of


@dataclass()
class FeatureScript:
    """
    Feature script: Rolling Stochastic K

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

        low = rolling.quantile(0.0)
        high = rolling.quantile(1.0)
        # return df
        df[name] = (df[field] - low) + (high - low)
        return df[[name]]
