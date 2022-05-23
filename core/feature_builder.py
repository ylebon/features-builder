import importlib

import time
from logbook import Logger
from pydantic import BaseModel
from typing import List

from features.scripts import FeatureScript


class FeatureBuilder(BaseModel):
    """
    Feature builder

    """
    name: str
    module: str
    params: dict
    columns: List[str]
    tags: List[str] = []
    script: FeatureScript = None
    log: Logger = None

    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def from_data(self, data):
        """
        From data

        """
        feature = FeatureBuilder(**data)
        feature._load_module()
        return feature

    def _load_module(self):
        """
        Load all

        """
        self.log = Logger(self.__class__.__name__)
        self.log.debug(f"msg='loading module' name='{self.name}'")
        module = importlib.import_module(self.module)
        self.script = module.FeatureScript(**self.params)
        self.log.debug(f"msg='module loaded' name='{self.name}'")

    def create_dataset(self, name, df):
        """
        Create dataset

        """
        self.log.info(f"msg='creating dataset' name='{self.name}' columns='{self.columns}'")
        start_time = time.time()
        df = df[self.columns]
        dataset = self.script.create_dataset(name, df)
        self.log.info(f"msg='dataset created' name='{self.name}' duration='{time.time() - start_time}'")
        return dataset


if __name__ == "__main__":
    from logbook import StreamHandler
    import sys

    StreamHandler(sys.stdout).push_application()
    data = {
        "name": "ask_qty__rolling_mean__300",
        "columns": ["ask_qty"],
        "module": "features.scripts.rolling_mean",
        "params": {
            "window": "300s",
            "resample": "1s"
        }
    }
    feature_builder = FeatureBuilder.from_data(data)
    feature_builder.create_dataset("")
