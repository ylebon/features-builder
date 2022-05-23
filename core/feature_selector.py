import os
from dataclasses import dataclass

import toml



@dataclass
class FeatureSelector(object):
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
        builder_list = dict()
        for file_name in os.listdir(features_definition):
            file_path = os.path.join(features_definition, file_name)
            data = toml.load(file_path)
            for key, value in data.items():
                if key == feature_name:
                    value["name"] = key
                    return value
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
        for file_name in os.listdir(features_definition):
            file_path = os.path.join(features_definition, file_name)
            data = toml.load(file_path)
            for feature in features:
                for name, value in data.items():
                    feature_tags = value.get("tags", [])
                    if feature.startswith("@") and feature in feature_tags:
                        name_list.append(name)
                    elif feature == name:
                        name_list.append(name)
        return name_list


if __name__ == "__main__":
    names = features_loader = FeatureLoader.get_names(
        "/Users/madazone/Workspace/varatra/signaler/features/features",
        tags="@variation"
    )
    print(names)
