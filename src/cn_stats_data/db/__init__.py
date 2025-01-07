import pathlib
import tomllib
from typing import Any

import psycopg2

from cn_stats_data.db.db_config import DbConfig

__all__ = ['db_config', 'metric_code_dao', 'metric_data_dao', 'region_code_dao', 'process_data_dao', 'models']


def _get_db_config(cfg: dict[str, Any]) -> DbConfig:
    return DbConfig(
        server=cfg['server'],
        port=cfg['port'],
        db=cfg['db'],
        user=cfg['user'],
        password=cfg['password'])


with (pathlib.Path(__file__).parent / "config.toml").open(mode="rb") as fp:
    _config = tomllib.load(fp)
    db_config = _get_db_config(_config['db'])


def get_conn():
    return psycopg2.connect(
        database=db_config.db,
        user=db_config.user,
        password=db_config.password,
        host=db_config.server,
        port=db_config.port)


if __name__ == '__main__':
    print(_config)
    print(db_config)
