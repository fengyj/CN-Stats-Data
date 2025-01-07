from dataclasses import dataclass

__all__ = ['DbConfig']


@dataclass
class DbConfig:
    server: str
    port: int
    db: str
    user: str
    password: str 
