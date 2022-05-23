import importlib
import os

import toml
from logbook import Logger


class FeaturesGenerator(object):
    """
    Features Generator

    """
    MAP_TIME_SHIFT = {
        "5m": 300,
        "10m": 600,
        "15m": 900,
        "20m": 1200,
        "25m": 1500,
        "30m": 1800,
        "35m": 2100,
        "40m": 2400,
        "45m": 2700,
        "50m": 3000,
        "55m": 3300,
        "60m": 3600,
    }

    def __init__(self, scripts_dir, output_dir):
        self.scripts_dir = scripts_dir
        self.output_dir = output_dir
        self.modules = list()
        self.templates = list()
        self.count = 0
        self.log = Logger("FeaturesGenerator")

    @classmethod
    def create(cls):
        """
        Create features

        """
        scripts_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
        output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'features'))
        return FeaturesGenerator(scripts_dir, output_dir)

    def load_scripts(self):
        """
        Load scripts

        """
        for script in os.listdir(self.scripts_dir):
            if script not in ['__init__.py', '__pycache__']:
                module_name = f"features.scripts.{script.replace('.py', '')}"
                module = importlib.import_module(module_name)
                self.modules.append(module)

    def generate(self):
        """
        Generate features

        """
        self.load_scripts()
        count = 0
        for module in self.modules:
            feature_list = self.create_features_toml(module)
            if feature_list:
                count += len(feature_list)

        self.log.info(f"msg='features generator finished' count={count}")

    def create_features_toml(self, module):
        """
        Create features TOML

        """
        name = module.__name__.split(".")[-1]
        self.log.info(f"msg='creating features module' name='{name}'")

        feature_list = list()

        if name in ['expected_profit_binary_rolling', 'expected_profit_rolling', 'expected_profit_range_rolling']:

            # template
            template = """
            [{name}__{time}]
            columns = ["ask_price", "bid_price"]
            module = "features.scripts.{name}"
            tags = ["@{name}", "@{name}.{time}"]
            [{name}__{time}.params]
            shift = {shift}
            """

            # create data
            time_list = FeaturesGenerator.MAP_TIME_SHIFT.keys()
            feature_list = []
            for time in time_list:
                data = template.format(name=name, time=time, shift=FeaturesGenerator.MAP_TIME_SHIFT.get(time))
                feature_list.append(data)

        elif name in ['field_date']:

            # template
            template = """
            [{name}__{field}]
            columns = ["date"]
            module = "features.scripts.{name}"
            tags = ["@{name}", "@{name}.{field}", "@variation"]
            [{name}__{field}.params]
            field = "{field}"
            """

            # create data
            field_list = ["month", "day", "hour", "minute", "second"]
            for field in field_list:
                data = template.format(name=name, field=field)
                feature_list.append(data)

        elif name in ['field_diff']:

            # template
            template = """
            [{name}__{field}]
            columns = ["{field}"]
            module = "features.scripts.{name}"
            tags = ["@{name}", "@{name}.{field}", "@variation"]
            [{name}__{field}.params]
            """

            # create data
            field_list = ["ask_price", "bid_price", "ask_qty", "bid_qty"]
            for field in field_list:
                data = template.format(name=name, field=field)
                feature_list.append(data)

        elif name in ['spread']:

            # template
            template = """
            [{name}]
            columns = ["ask_price", "bid_price"]
            module = "features.scripts.{name}"
            tags = ["@{name}", "@variation"]
            [{name}.params]
            """

            # create data
            data = template.format(name=name)
            feature_list.append(data)

        elif name in ['spread_rolling_mean', 'spread_rolling_std', 'spread_rolling_q50', 'spread_rolling_cov',
                      'spread_rolling_var']:

            # template
            template = """
            [{name}__{time}]
            columns = ["ask_price", "bid_price"]
            module = "features.scripts.{name}"
            tags = ["@{name}", "@{name}.5m"]
            [{name}__{time}.params]
            window = "{window}s"
            """

            # create data
            time_list = FeaturesGenerator.MAP_TIME_SHIFT.keys()
            for time in time_list:
                data = template.format(name=name, time=time, window=FeaturesGenerator.MAP_TIME_SHIFT.get(time))
                feature_list.append(data)

        elif name.startswith("field_diff"):

            # template
            template = """
            [{field}__{name}__{time}]
            columns = ["{field}"]
            module = "features.scripts.{name}"
            tags = ["@{name}", "@{field}.{name}", "@{field}.{name}.{time}", "@{group}.{name}", "@{group}.{name}.{time}"]
            [{field}__{name}__{time}.params]
            window = "{window}s"
            """

            # create data
            time_list = FeaturesGenerator.MAP_TIME_SHIFT.keys()
            for field in ['ask_price', 'bid_price', 'bid_qty', 'ask_qty']:
                group = 'price' if field in ['ask_price', 'bid_price'] else 'qty'
                for time in time_list:
                    data = template.format(name=name, time=time, field=field,
                                           window=FeaturesGenerator.MAP_TIME_SHIFT.get(time), group=group)
                    feature_list.append(data)

        elif name.startswith("field_"):

            # template
            template = """
            [{field}__{name}__{time}]
            columns = ["{field}"]
            module = "features.scripts.{name}"
            tags = ["@{name}", "@{field}.{name}", "@{field}.{name}.{time}", "@{group}.{name}", "@{group}.{name}.{time}"]
            [{field}__{name}__{time}.params]
            window = "{window}s"
            """

            # create data
            time_list = FeaturesGenerator.MAP_TIME_SHIFT.keys()
            for field in ['ask_price', 'bid_price', 'bid_qty', 'ask_qty']:
                group = 'price' if field in ['ask_price', 'bid_price'] else 'qty'
                for time in time_list:
                    data = template.format(name=name, time=time, field=field,
                                           window=FeaturesGenerator.MAP_TIME_SHIFT.get(time), group=group)
                    feature_list.append(data)

        elif name in ["profit_rolling", "profit_rolling_nofee"]:
            # template
            template = """
                   [profit_{shift}__{name}]
                   columns = ["bid_price", "ask_price"]
                   module = "features.scripts.{name}"
                   tags = ["@profit_{shift}", "@{name}", "@profit_{shift}.{name}"]
                   [profit_{shift}__{name}.params]
                   shift = {shift}
                   """
            shift_list = [300, 600, 900, 1200, 1500, 1800, 2100, 2400, 2700, 3000, 3300, 3600]
            for shift in shift_list:
                data = template.format(name=name, shift=shift)
                feature_list.append(data)

        elif name.startswith("profit_"):

            # template
            template = """
            [profit_{shift}__{name}__{time}]
            columns = ["bid_price", "ask_price"]
            module = "features.scripts.{name}"
            tags = ["@profit_{shift}", "@{name}", "@{name}.{time}", "@profit_{shift}.{name}", "@profit_{shift}.{name}.{time}"]
            [profit_{shift}__{name}__{time}.params]
            shift = {shift}
            window = "{window}s"
            """
            time_list = FeaturesGenerator.MAP_TIME_SHIFT.keys()
            shift_list = [300, 600, 1200]
            for time in time_list:
                for shift in shift_list:
                    data = template.format(name=name, time=time, shift=shift,
                                           window=FeaturesGenerator.MAP_TIME_SHIFT.get(time))
                    feature_list.append(data)


        else:
            self.log.error(f"msg='not implemented features generator' name='{name}'")

        #
        if feature_list:
            content_toml = "\n".join(feature_list)
            toml_content = toml.loads(content_toml)
            toml_file = os.path.join(self.output_dir, f"{name}.toml")
            with open(toml_file, 'w') as fw:
                toml.dump(toml_content, fw)

            return feature_list


if __name__ == "__main__":
    from logbook import StreamHandler
    import sys

    StreamHandler(sys.stdout).push_application()
    features_generator = FeaturesGenerator.create()
    features_generator.generate()
