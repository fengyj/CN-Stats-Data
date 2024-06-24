from dataclasses import dataclass
from typing import Self, ClassVar

from data.abstract import CachableData
from data.db_code import DbCode


@dataclass
class RegionCode(CachableData):
    """
    Represents a region code in CN-Stats
    """

    code: str = None
    db_code: DbCode = None
    name: str | None = None
    explanation: str | None = None
    children: list[Self] | None = None
    is_deleted: bool = False
    valid_attribute_names: ClassVar[set[str]] = {'english_name', 'english_explanation'}

    @staticmethod
    def __new__(cls,
                code: str,
                db_code: DbCode,
                name: str | None,
                children: list | None = None,
                is_deleted: bool = False,
                **kwargs):
        """
        Create or get an existing metric code object (depends on the metric_code, and db_code
        :param code: the code of the region
        :param db_code: the metric belongs to which DbCode
        :param name: the name of the region
        :param children: the children regions, if has
        :param is_deleted: is the data not available in the source or not
        :param kwargs: other properties of the code: name, english_name,
            created_time, last_updated_time
        """
        if code is None or db_code is None:
            raise ValueError('code %s or db_cde %s must not be None.' % (code, db_code))
        return super().__new__(cls, (code, db_code), **kwargs)

    def __init__(self,
                 code: str,
                 db_code: DbCode,
                 name: str | None,
                 children: list | None = None,
                 is_deleted: bool = False,
                 **kwargs) -> None:
        """
        Init the region code
        :param code: The code defined in CN-Stats
        :param db_code: the metric belongs to which DbCode
        :param name: the name of the metric
        :param children: The children region objects or the reg_code of the children regions.
            It could be None if it is a bottom level region.
        :param is_deleted: is the data not available in the source or not
        :param kwargs: other properties of the code: name, english_name,
            created_time, last_updated_time
        """
        super().__init__(**kwargs)

        self.code = code
        self.db_code = db_code
        if self.children is None and children is not None and isinstance(children, list):
            lst = []
            for c in children:
                if isinstance(c, RegionCode):
                    lst.append(c)
                elif isinstance(c, str):
                    lst.append(RegionCode(
                        code=c,
                        db_code=db_code,
                        name=None))
            if len(lst) > 0:
                self.children = lst

        if self.name is None and name is not None:
            self.name = name
        explanation = kwargs.get('explanation')
        if self.explanation is None and explanation is not None:
            self.explanation = explanation

        self.is_deleted = is_deleted

        pass

    def __eq__(self, __value, *args, **kwargs) -> bool:
        """
        Checks the equality based on the region code
        :param __value: another object to compare
        :return: Returns true if the metric codes are equal
        """
        if __value is None or not isinstance(__value, RegionCode):
            return False
        return self.code == __value.code \
            and self.db_code == __value.db_code

    def __hash__(self, *args, **kwargs) -> int:
        """
        Get hash code of the metric
        :param args:
        :param kwargs:
        :return: Returns the hash code based on the metric_code, and db_code.
        """
        return (self.code, self.db_code).__hash__()

    def __str__(self) -> str:
        """
        Get the string of the object
        :return: Returns the string of the object
        """
        return (f'RegionCode('
                f'code={self.code}, '
                f'db_code={None if self.db_code is None else self.db_code.code}, '
                f'name={self.name}, '
                f'explanation={self.explanation}, '
                f'children_region_codes={None if self.children is None else ','.join([i.code for i in self.children])}, '
                f'is_deleted={self.is_deleted}'
                f'{super().__str__()})@{id(self)}')
