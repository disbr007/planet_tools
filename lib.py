import os
import pathlib
import json

config_file = os.path.join(os.path.dirname(__file__), "config", "config.json")


def get_config(param):
    config_params = json.load(open(config_file))
    try:
        config = config_params[param]
    except KeyError:
        print('Config parameter not found: {}'.format(param))

    return config



