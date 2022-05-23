from dataclasses import dataclass


@dataclass
class FeatureScript:
    """
    Feature script: Return date field

    """
    field: str = None

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
        field = df.columns[0]
        if self.field == 'month':
            df[name] = df[field].dt.month
        elif self.field == 'day':
            df[name] = df[field].dt.day
        elif self.field == 'hour':
            df[name] = df[field].dt.hour
        elif self.field == 'minute':
            df[name] = df[field].dt.minute
        elif self.field == 'second':
            df[name] = df[field].dt.second
        return df[[name]]
