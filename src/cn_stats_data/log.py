from logging.config import dictConfig
import os
from yaml import load

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


def init_log_config() -> None:
    config_path = os.path.join(os.path.dirname(__file__), 'log.yml')
    with open(config_path, mode='r') as obj:
        logging_config = load(obj, Loader=Loader)
    dictConfig(logging_config)