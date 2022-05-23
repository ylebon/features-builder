from dataclasses import dataclass


@dataclass
class FeatureScript(object):
    """
    Execute feature script

    """

    def execute(self, timestamp, value):
        """
        Realtime execution

        """
        pass

    def create_dataset(self):
        """
        Create dataset

        """
        pass

