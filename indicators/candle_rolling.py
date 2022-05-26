from dataclasses import dataclass

from indicators.ts_container import TimeSeriesContainer


@dataclass
class CandleRolling(object):
    """
    Candle Time

    TODO: backfill is too slow
    """

    size: int
    container: TimeSeriesContainer = None
    full: bool = False
    backfill: bool = False

    def update(self, element):
        """
        Update price

        """
        if not element.date or not element.value:
            return
        try:
            date, value = self.container.get_index(0)
        except IndexError:
            self.container.update(element.date, element.value)
        else:
            elapsed_time = (element.date - date).total_seconds()

            # update elapsed time
            if elapsed_time > self.size:
                self.full = True
                self.container.remove_left()
                self.update(element)

            # update container with the recent value
            # elif self.backfill:
            #     for seconds in reversed(range(int(elapsed_time))):
            #         dt = element.date - timedelta(seconds=seconds)
            #         self.container.update(dt, element.value)

            # update container with the value
            else:
                self.container.update(element.date, element.value)

    def is_full(self):
        """
        Check candle rolling is full

        """
        return self.full

    def is_empty(self):
        """
        Check candle rolling is full

        """
        return self.container.is_empty()

    @classmethod
    def from_empty(cls, size, backfill=False):
        """
        From empty

        """
        candle = CandleRolling(size=size, container=TimeSeriesContainer(), backfill=backfill)
        return candle

    def clean(self):
        """
        Clean candle rolling

        """
        self.container = TimeSeriesContainer()
        self.full = False