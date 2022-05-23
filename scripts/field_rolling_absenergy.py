from dataclasses import dataclass

import numpy as np
from logbook import Logger


@dataclass
class FeatureScript:
    """
    Absolute energy

    """
    window: str
    resample: int = None
    log: int = None

    def __post_init__(self):
        """
        Post init

        """
        self.log = Logger("[feature] abs_energy")

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
        pass

    def create_dataset(self, name, df):
        """
        Create DataSet

        """
        field = df.columns[0]
        df[name] = df[field].rolling(self.window, min_periods=self.window_int).apply(lambda x: np.dot(x, x), raw=False)
        return df[[name]]
