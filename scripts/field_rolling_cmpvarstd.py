from dataclasses import dataclass

import numpy as np


@dataclass()
class FeatureScript:
    """
    Feature script: Variance vs Standard deviation

    """
    window: int
    resample: int = None
    diff: bool = False

    def __post_init__(self):
        """
        Post init

        """
        pass

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

        def fn(x):
            y = np.var(x)
            return y > np.sqrt(y)

        df[name] = df[field].rolling(self.window, min_periods=self.window_int).apply(fn, raw=False)
        return df[[name]]
