from dataclasses import dataclass

from hamcrest import assert_that, instance_of

Template = """


"""

@dataclass
class FeatureScript:
    """
    Feature script: Rolling Profit Coeff

    """
    window: str
    shift: float = 0.0
    fee: float = 0.001

    def __post_init__(self):
        """
        Post init

        """
        assert_that(self.shift, instance_of(int))
        self.shift = abs(self.shift)

    @property
    def window_int(self):
        """
        Window integer

        """
        return int(self.window.replace("s", ""))

    def create_dataset(self, name, df):
        """
        Create DataSet

        """
        sell_price = df['bid_price'].shift(self.shift)
        buy_price = df['ask_price']

        # quantity in
        qty_in = 1

        # quantity after buy
        qty_in_1 = qty_in / buy_price
        qty_in_2 = qty_in_1 - (qty_in_1 * self.fee)

        # quantity after sell
        qty_out_1 = qty_in_2 * sell_price
        qty_out = qty_out_1 - (qty_out_1 * self.fee)

        # profit
        profit = qty_out - qty_in

        # profit ratio
        ratio_series = profit / qty_in
        rolling_ratio_series = ratio_series.rolling(self.window, min_periods=self.window_int)
        rolling_q_25 = rolling_ratio_series.quantile(0.25)
        rolling_q_75 = rolling_ratio_series.quantile(0.75)

        df[name] = (rolling_q_75 - rolling_q_25) / (rolling_q_75 + rolling_q_25)
        return df[[name]]
