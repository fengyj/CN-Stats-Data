from dataclasses import dataclass
from data.abstract import CachableData
from data.db_code import DbCode
from typing import Self, Any, ClassVar
from functools import cache


@dataclass
class MetricCode(CachableData):
    """
    Represents a metric in CN-Stats
    """

    code: str = None
    db_code: DbCode = None
    name: str | None = None
    explanation: str | None = None
    memo: str | None = None
    unit: str | None = None
    parent: Self | None = None
    is_deleted: bool = False
    children: dict[str, Self] | None = None
    valid_attribute_names: ClassVar[set[str]] = {'english_name', 'english_explanation', 'english_memo', 'english_unit'}

    @staticmethod
    def __new__(cls,
                code: str,
                db_code: DbCode,
                name: str | None,
                parent=None,
                is_deleted: bool = False,
                **kwargs: dict[str, Any]):
        """
        Create or get an existing metric code object (depends on the metric_code, and db_code
        :param code: the code of the metric
        :param db_code: the metric belongs to which DbCode
        :param name: the name of the metric
        :param parent: the parent metric, if has
        :param is_deleted: is the data not available in the source or not
        :param kwargs: other properties of the code: name, english_name, explanation, memo,
            created_time, last_updated_time
        """
        if code is None or db_code is None:
            raise ValueError('code %s or db_cde %s must not be None.' % (code, db_code))
        return super().__new__(cls, (code, db_code), **kwargs)

    def __init__(self,
                 code: str,
                 db_code: DbCode,
                 name: str | None,
                 parent: Self | str | None = None,
                 is_deleted: bool = False,
                 **kwargs) -> None:
        """
        Init the metric code
        :param code: The code defined in CN-Stats
        :param db_code: The metric belongs to which DbCode
        :param name: the name of the metric
        :param parent: The parent metric object or the metric_code of the parent metric.
            It could be None if it is a top level metric.
        :param is_deleted: is the data not available in the source or not
        :param kwargs: other properties of the code: name, english_name, explanation, memo,
            created_time, last_updated_time
        """

        super().__init__(**kwargs)

        self.code = code
        self.db_code = db_code
        if isinstance(parent, str):
            parent = MetricCode(parent, db_code, None, None)
        if self.parent is None and parent is not None:
            self.parent = parent
            if parent.children is None:
                parent.children = {self.code: self}
            else:
                parent.children[self.code] = self

        if self.name is None and name is not None:
            self.name = name
        explanation = kwargs.get('explanation')
        if self.explanation is None and explanation is not None:
            self.explanation = explanation
        memo = kwargs.get('memo')
        if self.memo is None and memo is not None:
            self.memo = memo
        unit = kwargs.get('unit')
        if self.unit is None and unit is not None:
            self.unit = unit

        self.is_deleted = is_deleted

        pass

    def __eq__(self, __value, *args, **kwargs) -> bool:
        """
        Checks the equality based on the metric code and db code
        :param __value: another object to compare
        :return: Returns true if the metric code and db code are equal
        """
        if __value is None or not isinstance(__value, MetricCode):
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
        return (f'MetricCode('
                f'code={self.code}, '
                f'db_code={None if self.db_code is None else self.db_code.code}, '
                f'name={self.name}, '
                f'explanation={self.explanation}, '
                f'memo={self.memo}, '
                f'unit={self.unit}, '
                f'parent_metric_code={None if self.parent is None else self.parent.code}, '
                f'is_deleted={self.is_deleted}'
                f'{super().__str__()})@{id(self)}')

    @cache
    def get_non_parent_metrics(self) -> list[Self]:
        if self.children is None:
            return [self]
        else:
            result = []
            for c in self.children.values():
                result = result + c.get_non_parent_metrics()
            return result

    #
    # @staticmethod
    # def import_metrics(db_code: DbCode | None = None) -> None:
    #     """
    #     Imports metric codes from getTree API to database
    #     :param db_code: The db_code of the metrics. If it's None, means to import all metric codes
    #     :return: None
    #     """
    #     db_codes: list[DbCode] = list(DbCode) if db_code is None else [db_code]
    #     for code in db_codes:
    #         print(f'Starting to download metric codes under {code.code} - {code.name}.')
    #         lst = cn_stats_data.data.dao.StatsRawDataDao.download_metrics(code)
    #         print(f'Retrieved {len(lst)} codes.')
    #         if len(lst) > 0:
    #             MetricCode.add_or_update(lst)
    #             print('Saved the codes.')
