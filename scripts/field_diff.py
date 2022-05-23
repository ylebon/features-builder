from dataclasses import dataclass


@dataclass
class FeatureScript:
    """
    Feature script: diff

    """
    resample: int = None

    def create_dataset(self, name, df):
        """
        Create DataSet

        """
        field = df.columns[0]
        df[name] = df[field].diff()
        return df[[name]]
