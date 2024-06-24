import dataclasses
import json
import time
from typing import Any

import requests
from functools import cache
from dataclasses import dataclass


@dataclass
class MetricCodeResponseData:
    code: str
    db_code: str
    name: str
    parent: str | None
    exp: str | None
    memo: str | None
    unit: str | None
    is_parent: bool


@dataclass
class RegionCodeResponseData:
    code: str
    db_code: str
    name: str
    parent: str | None
    exp: str | None
    is_parent: bool
    children: list[str] | None


@dataclass
class MetricDataResponseNode:
    name: str
    cname: str | None
    code: str
    exp: str | None
    memo: str | None
    tag: str | None
    unit: str | None

    def __init__(self, **kwargs):
        if 'name' in kwargs:
            self.name = kwargs['name']
        if 'cname' in kwargs:
            self.cname = kwargs['cname']
        if 'code' in kwargs:
            self.code = kwargs['code']
        if 'exp' in kwargs:
            self.exp = kwargs['exp']
        if 'memo' in kwargs:
            self.memo = kwargs['memo']
        if 'tag' in kwargs:
            self.tag = kwargs['tag']
        if 'unit' in kwargs:
            self.unit = kwargs['unit']
        pass

@dataclass
class MetricDataResponseData:
    data: float = None
    has_data: bool = False
    str_data: str | None = None
    metric_node: MetricDataResponseNode = None
    metric_code: str = None
    date_node: MetricDataResponseNode = None
    date_code: str = None
    region_node: MetricDataResponseNode | None = None
    region_code: str | None = None

    def __init__(self, data: float, has_data: bool, str_data: str | None):
        self.data = data
        self.has_data = has_data
        self.str_data = str_data
        pass
