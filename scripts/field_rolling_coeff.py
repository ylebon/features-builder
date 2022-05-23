from copy import copy
from dataclasses import dataclass

from logbook import Logger

from indicators.core.candle.candle_element import CandleElement
from indicators.core.candle.candle_rolling import CandleRolling


@dataclass()
class FeatureScript:
    """
    Feature script: Rolling Coefficient

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
        self.log = Logger("feature - rolling coeff")
        self.candle = CandleRolling.from_empty(self.window_int)
        self.log.info(f"window='{self.window_int}'")

    def execute(self, dt, value):
        """
        Execute Profit

        """
        candle_element = CandleElement(dt, value)
        self.candle.update(candle_element)
        if not self.candle.full:
            return None
        else:
            rolling = self.candle.container
            rolling_q_25 = rolling.get_percentile(25)
            rolling_q_75 = rolling.get_percentile(75)
            return (rolling_q_75 - rolling_q_25) / (rolling_q_75 + rolling_q_25)

    def create_dataset(self, name, df):
        """
        Create DataSet

        """
        field = df.columns[0]
        rolling = df[field].rolling(self.window, min_periods=self.window_int)
        rolling_q_25 = rolling.quantile(0.25)
        rolling_q_75 = rolling.quantile(0.75)

        df[name] = (rolling_q_75 - rolling_q_25) / (rolling_q_75 + rolling_q_25)
        return df[[name]]
