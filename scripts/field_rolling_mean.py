from copy import copy
from dataclasses import dataclass

from logbook import Logger
from sklearn import preprocessing

from indicators.core.candle.candle_element import CandleElement
from indicators.core.candle.candle_rolling import CandleRolling


@dataclass
class FeatureScript:
    """
    Rolling Mean

    """
    window: str
    resample: str = None
    candle: int = None
    log: int = None
    diff: int = 0
    remove_outliers: bool = False
    scale: bool = True
    scaler = preprocessing.MinMaxScaler()

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
        df_mean = df[field].rolling(self.window, min_periods=self.window_int).mean()

        # check diff mean
        if self.diff:
            df[name] = df - df_mean
        else:
            df[name] = df_mean
        return df[[name]]
