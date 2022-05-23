import numpy as np
from dataclasses import dataclass
from hamcrest import assert_that, instance_of
from sklearn.preprocessing import LabelEncoder


def pattern_change(x):
    """
    Pattern Change

    """
    size = 5
    window = "180S"
    candles = x.resample(window).ohlc()[-size:]

    candles['open_close'] = (candles['close'] > candles['open']).astype(int).astype(str)
    candles['open_high'] = (candles['open'] > candles['high']).astype(int).astype(str)
    candles['open_low'] = (candles['open'] > candles['low']).astype(int).astype(str)
    candles['close_high'] = (candles['close'] > candles['high']).astype(int).astype(str)
    candles['close_low'] = (candles['close'] > candles['low']).astype(int).astype(str)
    candles['pattern'] = candles['open_high'] + candles['open_close'] + candles['open_low'] + candles['close_high'] + \
                         candles['close_low']

    pattern = ""
    for v in candles['pattern'].values:
        pattern += v
    return int(pattern, 2)


def pattern_ohlc(x, indexes=[-1, -2, -3, -4, -5]):
    """
    OHLC Pattern Builder

    """
    pattern_list = list()
    candles = x.resample('300s').ohlc()
    for index in indexes:
        s = ""
        s += str(int(candles[index]['open'] > candles[index]['close']))
        s += str(int(True))
        s += str(int(True))
        s += str(int(True))
        pattern_list.append(s)
    # pattern bin
    pattern_bin = ".".join(pattern_list)
    # convert to hex
    pattern_hex = hex(int((pattern_bin.replace(".", "")), 2))
    # convert to int
    return int(pattern_hex, 0)


def pattern_default(x):
    """
    Pattern encoder

    """
    pattern_list = list()
    # open / close
    a = str(int(x[-1] > x[0]))
    pattern_list.append(a)
    # mean / median
    b = str(int(np.mean(x) > np.median(x)))
    pattern_list.append(b)
    # up / down
    diff = np.diff(x)
    c = str(int(np.sum(diff >= 0) > x.size / 2))
    pattern_list.append(c)
    # pattern
    return int("".join(pattern_list), 2)


@dataclass
class FeatureScript:
    """
    Feature script: Pattern creator

    """
    window: int
    pattern: str = None
    resample: str = "1s"
    encoders: int = 0

    def __post_init__(self):
        assert_that(self.window, instance_of(str))
        self.encoders = {
            'ohlc': pattern_ohlc,
            'default': pattern_default
        }

    def execute(self, timestamp, value):
        """
        Realtime Execution

        """
        pass

    def create_dataset(self, df):
        """
        Return close price

        """
        min_periods = int(self.window.replace("s", ""))
        rolling_data = df.resample(self.resample).bfill().rolling(self.window, min_periods=min_periods).agg({
            'pattern': pattern_change
        })
        try:
            result = rolling_data['pattern']['bid_price']
        except:
            result = rolling_data['pattern']['ask_qty']

        # label encoder
        return result.astype('category')

    def encoder(self):
        """
        Label encoder

        """
        pass
