from dataclasses import dataclass

from logbook import Logger


@dataclass
class FeatureScript:
    """
    Rolling Compare Open Low

    """
    window: str
    resample: str = None
    log: int = None

    def __post_init__(self):
        """
        Post init

        """
        self.log = Logger("feature - rolling cmp open low")
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
        pass

    def create_dataset(self, name, df):
        """
        Create DataSet

        """
        field = df.columns[0]
        rolling = df[field].rolling(self.window, min_periods=self.window_int)
        rolling_open = df[field].shift(self.window_int)
        rolling_min = rolling.quantile(0.0)
        df[name] = rolling_open < rolling_min

        return df[[name]]

