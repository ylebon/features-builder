from dataclasses import dataclass

from logbook import Logger


@dataclass
class FeatureScript:
    """
    Rolling Compare Open Close

    """
    window: str
    resample: str = None
    log: int = None

    def __post_init__(self):
        """
        Post init

        """
        self.log = Logger("feature - rolling cmp open close")
        self.log.info(f"window='{self.window_int}'")

    @property
    def window_int(self):
        """
        Window integer

        """
        return abs(int(self.window.replace("s", "")))

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
        rolling_open = df[field].shift(self.window_int)
        rolling_close = df[field]
        df[name] = rolling_open < rolling_close
        return df[[name]]
