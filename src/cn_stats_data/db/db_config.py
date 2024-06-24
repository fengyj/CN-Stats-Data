from dataclasses import dataclass

__all__ = ['DbConfig']


@dataclass
class DbConfig:
    server: str = None
    port: int = None
    db: str = None
    user: str = None
    password: str = None
