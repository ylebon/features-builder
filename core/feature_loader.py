import os
from dataclasses import dataclass

import toml

from core.feature_builder import FeatureBuilder


class FeatureDefinitionNotFound(Exception):
    """
    Feature definition not found

    """
    pass


@dataclass
class FeatureLoader(object):
    """
    Feature loader

    """
    builder_list: dict

    @classmethod
    def from_file(cls, features_definition):
        """
        Features loader from file

        """
        builder_list = dict()
        for file_name in os.listdir(features_definition):
            file_path = os.path.join(features_definition, file_name)
            data = toml.load(file_path)
            for key, value in data.items():
                value["name"] = key
                feature_builder = FeatureBuilder.from_data(value)
                builder_list[key] = feature_builder
        # features loader
        return FeatureLoader(builder_list)

    @classmethod
    def load_data(cls, features_definition, feature_name):
        """
        Features loader from file

        """
        for file_name in os.listdir(features_definition):
            file_path = os.path.join(features_definition, file_name)
            data = toml.load(file_path)
            for key, value in data.items():
                if key == feature_name:
                    value["name"] = key
                    return value
        raise FeatureDefinitionNotFound(f"{feature_name}")

    @classmethod
    def get_names(cls, features_definition):
        """
        Features loader from file

        """
        name_list = list()
        for file_name in os.listdir(features_definition):
            file_path = os.path.join(features_definition, file_name)
            data = toml.load(file_path)
            name_list.extend([name for name, value in data.items()])
        return name_list

    @classmethod
    def filter_features(cls, features_definition, features):
        """
        Filter tags

        """
        name_list = list()
        if features:
            for file_name in os.listdir(features_definition):
                file_path = os.path.join(features_definition, file_name)
                data = toml.load(file_path)
                for name, value in data.items():
                    for feature in features:
                        if name == feature:
                            name_list.append(name)
                        elif feature.startswith("@") and feature in value.get("tags", []):
                            name_list.append(name)
        else:
            for file_name in os.listdir(features_definition):
                file_path = os.path.join(features_definition, file_name)
                data = toml.load(file_path)
                for name, value in data.items():
                    name_list.append(name)
        return list(set(name_list))

    @classmethod
    def get_data(cls, features_definition, feature):
        """
        Feature data

        """
        for file_name in os.listdir(features_definition):
            file_path = os.path.join(features_definition, file_name)
            data = toml.load(file_path)
            for name, value in data.items():
                if name == feature:
                    return value


if __name__ == "__main__":
    names = FeatureLoader.filter_features(
        "/Users/ylebon/Workspace/features-builder/features",
        ["@field_diff_rolling_median"]
    )

    print(names)