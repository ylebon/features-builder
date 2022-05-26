from dataclasses import dataclass
from datetime import datetime


@dataclass
class CandleElement:
    date: datetime
    value: float

    @classmethod
    def from_tuple(self, date, value):
        return CandleElement(date=date, value=value)

    def __str__(self):
        return str(self.__dict__)