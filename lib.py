import os
import pathlib
import json

config_file = os.path.join(os.path.dirname(__file__),"config", "aws_creds.json")


def get_config(param):
    config_params = json.load(open(config_file))
    config = config_params[param]

    return config
