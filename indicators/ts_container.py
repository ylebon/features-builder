import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class TimeSeriesContainer:
    x: np.array = np.array([], dtype=np.float16)
    t: np.array = np.array([], dtype=np.float16)
    size: int = 0

    # count negative / positive
    neg: int = 0
    pos: int = 0
    count: int = 0

    def update(self, date, value):
        """
        Update container
        """
        if value:
            self.t = np.append(self.t, date)
            self.x = np.append(self.x, value)

            # count neg / pos
            if value < 0:
                self.neg += 1
            else:
                self.pos += 1

            # count
            self.count += 1

    def get_values(self):
        """Return values"""
        return self.x

    def get_dates(self):
        """Return dates"""
        return self.t

    def get_index(self, index):
        """Return value at a certain index"""
        return self.t[index], self.x[index]

    def remove_left(self):
        """
        Remove left
        :return:
        """
        self.t = np.delete(self.t, [0])
        self.x = np.delete(self.x, [0])

    def get_rsi(self):
        """Return RSI"""
        diff = np.diff(self.x)
        neg = diff[diff < 0]
        pos = diff[diff > 0]
        try:
            rs = pos.size / neg.size
            rsi = (100.0 - (100.0 / (1.0 + rs)))
        except ZeroDivisionError:
            return None
        return rsi

    def get_weighted_average(self):
        """Return weighted average"""
        return np.ma.array(self.x)

    def get_mean(self):
        """Return mean"""
        return np.average(self.x)

    def get_ewma(self):
        """Return ewma"""
        weights = np.exp(np.linspace(-1., 0., self.x.size))
        weights /= weights.sum()
        return np.average(self.x, weights=weights)

    def get_median(self):
        """Return mean"""
        return np.median(self.x)

    def get_pct_change(self):
        """Return pct change"""
        pct_change = (float(self.x[-1]) - float(self.x[0])) / float(self.x[0])
        return pct_change

    def get_percentile(self, percentile):
        """Return percentile"""
        return np.percentile(self.x, percentile)

    def get_std(self):
        """Return standard deviation"""
        return np.std(self.x)

    def get_var(self):
        """Return variance"""
        return np.var(self.x)

    def get_bollinger(self, num_of_std=2):
        """Return bollinger"""
        mean = np.average(self.x)
        std = np.std(self.x)
        upper_band = mean + (std * num_of_std)
        lower_band = mean - (std * num_of_std)
        return mean, upper_band, lower_band

    def get_open(self):
        """Return open"""
        return self.x[0]

    def get_close(self):
        """Return close"""
        return self.x[-1]

    def get_open_time(self):
        """Return open"""
        return self.t[0]

    def get_close_time(self):
        """Return close"""
        return self.t[-1]

    def get_low(self):
        """Return low"""
        return np.min(self.x)

    def get_index_t(self, index):
        """Return index"""
        return self.t[index]

    def get_index_v(self, index):
        """Return index"""
        return self.x[index]

    def get_high(self):
        """Return high"""
        return np.max(self.x)

    def get_pivot(self):
        """Return pivot"""
        h = self.get_high()
        l = self.get_low()
        c = self.get_close()
        P = (h + l + c) / 3
        S1 = (2 * P) - h
        S2 = P - (h - l)
        R1 = (2 * P) - l
        R2 = P + (h - l)
        return P, R1, R2, S1, S2

    def go_down(self):
        """Check going down"""
        return self.x[0] > self.x[-1]

    def go_up(self):
        """Check going up"""
        return self.x[0] < self.x[-1]

    def get_diff(self):
        """Return first and last diff"""
        return self.x[-1] - self.x[0]

    def to_series(self):
        """
        Convert to pandas series

        """
        return pd.Series(data=self.x, index=self.t)

    def get_derivative(self):
        """
        Return derivative

        """
        return (self.x[-1] - self.x[0]) / (self.t[-1] - self.t[0]).total_seconds()

    def get_count(self):
        """
        Return count

        """
        return len(self.x)

    def is_empty(self):
        """
        Return empty

        """
        return self.t.size == 0

    def get_pct_neg(self):
        """
        Return neg pct

        """
        return self.neg / self.count

    def get_pct_pos(self):
        """
        Return pos pct

        """
        return self.pos / self.count
