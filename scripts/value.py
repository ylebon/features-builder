from dataclasses import dataclass


@dataclass
class FeatureScript:
    """
    Feature script: Value

    """

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
        df[name] = df[field].copy()
        return df[[name]]
