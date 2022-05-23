from dataclasses import dataclass


@dataclass
class FeatureScript:
    """
    Feature script: Return BID/ASK spread

    """
    resample: int = None

    def __post_init__(self):
        """
        Post init

        """
        pass

    def execute(self, ts, value):
        """
        Execute Profit

        """
        pass

    def create_dataset(self, name, df):
        """
        Create DataSet

        """
        df[name] = df['ask_price'] - df['bid_price']
        return df[[name]]
