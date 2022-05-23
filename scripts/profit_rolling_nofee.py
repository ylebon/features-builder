from dataclasses import dataclass

from hamcrest import assert_that, instance_of


@dataclass
class FeatureScript:
    """
    Feature script: Rolling Profit No Fee

    """
    shift: float = 0.0
    fee: float = 0.001

    def __post_init__(self):
        """
        Post init

        """
        assert_that(self.shift, instance_of(int))
        self.shift = abs(self.shift)

    def create_dataset(self, name, df):
        """
        Create DataSet

        """
        sell_price = df['bid_price'].shift(self.shift)
        buy_price = df['ask_price']

        # profit
        profit = (sell_price - buy_price)

        # profit ratio
        df[name] = profit / buy_price

        # result
        return df[[name]]
