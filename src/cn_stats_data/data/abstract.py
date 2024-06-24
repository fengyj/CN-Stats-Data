from dataclasses import dataclass
from typing import Self, Any, ClassVar
from datetime import datetime


@dataclass
class ExpandableData:
    extra_attributes: dict[str, Any | None]
    created_time: datetime | None = None
    last_updated_time: datetime | None = None
    valid_attribute_names: ClassVar[set[str]] = {}

    def __init__(self, **kwargs) -> None:

        if not hasattr(self, 'extra_attributes'):
            self.extra_attributes = self.get_valid_extra_attributes(kwargs)
        else:
            for k, v in self.get_valid_extra_attributes(kwargs):
                self.extra_attributes[k] = v

        created_time = kwargs.get('created_time')
        if self.created_time is None and created_time is not None:
            self.created_time = created_time
        last_updated_time = kwargs.get('last_updated_time')
        if self.last_updated_time is None and last_updated_time is not None:
            self.last_updated_time = last_updated_time

        pass

    def __str__(self) -> str:

        attributes = self.get_valid_extra_attributes(self.extra_attributes)
        if attributes is None or len(attributes) == 0:
            return ''
        else:
            return ', ' + ', '.join(f'{k}={v}' for (k, v) in attributes.items())

    @classmethod
    def get_valid_extra_attributes(cls, attributes: dict[str, Any | None] | None) -> dict[str, Any | None]:
        """
        Get dict only with the valid attributes for saving to database
        :return: the dict for saving
        """
        if attributes is None:
            return {}
        elif len(attributes) == 0:
            return attributes
        else:
            return {k: v for (k, v) in attributes.items() if k in cls.valid_attribute_names}


class CachableData(ExpandableData):
    _cache = dict()

    @staticmethod
    def __new__(cls, keys: tuple, **kwargs):

        if keys is None or len(keys) == 0:
            raise ValueError('keys must not be None or empty.')

        obj = cls._cache.get(keys)
        if obj is None:
            obj = super().__new__(cls)
            cls._cache[keys] = obj
        return obj

    @classmethod
    def get_all_cached_objects(cls) -> list[Self]:
        return [i for i in cls._cache.values()]
