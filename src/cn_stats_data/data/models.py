import dataclasses
from datetime import datetime
from enum import  Enum
from typing import Self, Any
from functools import cache

__all__ = ['DbCode', 'RegionCode', 'MetricCode', 'MetricData']


class DbCode(Enum):
    """
    An enumerable type for the CN-Stats dbcode.
    """

    MACRO_ECONOMIC_ANNUAL = 'hgnd', '宏观年度'
    MACRO_ECONOMIC_QUARTERLY = 'hgjd', '宏观季度'
    MACRO_ECONOMIC_MONTHLY = 'hgyd', '宏观月度'
    PROVINCE_ANNUAL = 'fsnd', '分省年度'
    PROVINCE_QUARTERLY = 'fsjd', '分省季度'
    PROVINCE_MONTHLY = 'fsyd', '分省月度'
    CITY_ANNUAL = 'csnd', '城市年度'
    HMT_ANNUAL = 'gatnd', '港澳台年度'
    # CITY_QUARTERLY = 'csjd', '城市季度' # NO city quarterly data
    CITY_MONTHLY = 'csyd', '城市月度'
    HMT_MONTHLY = 'gatyd', '港澳台月度'

    def __init__(self, code: str, name: str) -> None:
        """
        Init the dbcode
        :param code: The dbcode
        :param name: The name of the code
        """
        self._code = code
        self._name = name
        pass

    def __hash__(self, *args, **kwargs) -> int:
        """
        Get the hash code
        :return: The hash code
        """
        return self._code.__hash__()

    def __eq__(self, __value, *args, **kwargs) -> bool:
        """
        Checks the equality
        :param __value: Another object to check
        :return: Returns True if the codes are the same
        """
        if __value is None or not isinstance(__value, DbCode):
            return False
        return self._code.__eq__(__value._code)

    @property
    def code(self) -> str:
        """
        Returns the dbcode
        :return: dbcode
        """
        return self._code

    @property
    def name(self) -> str:
        """
        Returns the name of the dbcode
        :return: name of the dbcode
        """
        return self._name

    def is_annual(self) -> bool:
        """
        Checks the code if it represents a annual data
        :return: true if it's annual
        """
        return self._code[-2:] == 'nd'

    def is_quarterly(self) -> bool:
        """
        Checks the code if it represents a quarterly data
        :return: true if it's quarterly
        """
        return self._code[-2:] == 'jd'

    def is_monthly(self) -> bool:
        """
        Checks the code if it represents a monthly data
        :return: true if it's monthly
        """
        return self._code[-2:] == 'yd'

    def get_date_num(self, val: str) -> int:
        """
        Gets the number of the date which is used in database
        :param val: the date value got from CN-Stats.
            For annual codes, the value format is 'yyyy'.
            For quarterly codes, the value format is 'yyyy[A|B|C|D]'
            For monthly codes, the value format is 'yyyyMM'
        :return: For annual codes, returns the 'yyyy12' value
         For quarterly codes, returns the 'yyyyMM' value, the MM of each quarter is 03, 06, 09, and 12.
         For monthly codes, returns the 'yyyyMM' value.
        """
        if val is None:
            raise ValueError('Input cannot be None.')
        if self.is_annual():
            if len(val) != 4 or not val.isnumeric():
                raise ValueError('The input {val} is invalid.'.format(val=val))
            return int(val) * 100 + 12
        elif self.is_quarterly():
            if len(val) != 5 or val[4] not in ['A', 'B', 'C', 'D']:
                raise ValueError('The input {val} is invalid.'.format(val=val))
            return int(val[:4]) * 100 + (ord(val[4]) - ord('A') + 1) * 3
        elif self.is_monthly():
            if len(val) != 6:
                raise ValueError('The input {val} is invalid.'.format(val=val))
            return int(val)
        else:
            raise ValueError('Unknown code type {code} - {name}.'.format(code=self._code, name=self._name))

    def get_date_code(self, num: int) -> str:
        """
        Gets the date code of the date number for loading data from CN-Stats.
        :param num: the data number from database.
            For annual codes, the value should be 'yyyy12'.
            For quarterly codes, the value should be 'yyyyMM', the 'MM' should be 03, 06, 09, or 12.
            For monthly codes, the value should be 'yyyyMM'.
        :return: For annual codes, returns the 'yyyy' value.
            For quarterly codes, returns the 'yyyy[A|B|C|D]' value.
            For monthly codes, returns the 'yyyyMM' value.
        """
        if num is None:
            raise ValueError('Input cannot be None.')

        year, month = divmod(num, 100)
        if num < 1900 or num > 2100:
            raise ValueError('The year of the input {val} is not in the valid range.'.format(val=num))
        err: ValueError = ValueError('The month of the input {val} is not in the valid range.'.format(val=num))
        if self.is_annual():
            if month != 12:
                raise err
            return str(year)
        elif self.is_quarterly():
            if month not in {3, 6, 9, 12}:
                raise err
            return str(year) + 'ABCD'[(month - 1) // 3]
        elif self.is_monthly():
            if month < 1 or month > 12:
                raise err
            return str(num)
        else:
            raise ValueError('Unknown code type {code} - {name}.'.format(code=self._code, name=self._name))

    @classmethod
    @cache
    def get_macro_economic_codes(cls) -> list[Self]:
        """
        Get codes of macro economic level
        :return: Codes of macro economic level
        """
        return [cls.MACRO_ECONOMIC_ANNUAL, cls.MACRO_ECONOMIC_QUARTERLY, cls.MACRO_ECONOMIC_MONTHLY]

    @classmethod
    @cache
    def get_province_codes(cls) -> list[Self]:
        """
        Get codes of province level
        :return: Codes of province level
        """
        return [cls.PROVINCE_ANNUAL, cls.PROVINCE_QUARTERLY, cls.PROVINCE_MONTHLY]

    @classmethod
    @cache
    def get_city_codes(cls) -> list[Self]:
        """
        Get codes of city level
        :return: Codes of city level
        """
        return [cls.CITY_ANNUAL, cls.HMT_ANNUAL, cls.CITY_MONTHLY, cls.HMT_MONTHLY]

    @classmethod
    @cache
    def get_annual_codes(cls) -> list[Self]:
        """
        Get codes of annual level
        :return: Codes of annual level
        """
        return [cls.MACRO_ECONOMIC_ANNUAL, cls.PROVINCE_ANNUAL, cls.CITY_ANNUAL, cls.HMT_ANNUAL]

    @classmethod
    @cache
    def get_quarterly_codes(cls) -> list[Self]:
        """
        Get codes of quarterly level
        :return: Codes of quarterly level
        """
        return [cls.MACRO_ECONOMIC_QUARTERLY, cls.PROVINCE_QUARTERLY]

    @classmethod
    @cache
    def get_monthly_codes(cls) -> list[Self]:
        """
        Get codes of monthly level
        :return: Codes of monthly level
        """
        return [cls.MACRO_ECONOMIC_MONTHLY, cls.PROVINCE_MONTHLY, cls.CITY_MONTHLY, cls.HMT_MONTHLY]

    @classmethod
    @cache
    def get_code(cls, code: str) -> Self | None:
        """
        Get DbCode object via code
        :param code: the code of the DbCode
        :return: Returns the DbCode object if found, otherwise returns None
        """
        code_dict = {e.code: e for e in DbCode}
        return code_dict.get(code)


@dataclasses.dataclass
class ExpandableData:
    extra_attributes: dict[str, Any | None]
    created_time: datetime | None = None
    last_updated_time: datetime | None = None

    def __init__(self, **kwargs) -> None:

        allowed_attributes = self.extra_attribute_names()
        self.extra_attributes = {} if kwargs is None else {k: v for (k, v) in kwargs.items() if k in allowed_attributes}

        created_time = kwargs.get('created_time')
        if self.created_time is None and created_time is not None:
            self.created_time = created_time
        last_updated_time = kwargs.get('last_updated_time')
        if self.last_updated_time is None and last_updated_time is not None:
            self.last_updated_time = last_updated_time

        pass

    @classmethod
    def extra_attribute_names(cls) -> set[str]:
        return set()

    def __repr__(self) -> str:

        if self.extra_attributes is None or len(self.extra_attributes) == 0:
            return ''
        else:
            return ', ' + ', '.join(f'{k}={v}' for (k, v) in self.extra_attributes.items())


class CachableData(ExpandableData):
    _cache = dict()

    @staticmethod
    def __new__(cls, keys: tuple, **kwargs):

        if keys is None or len(keys) == 0:
            raise ValueError('keys must not be None or empty.')

        obj = cls._cache.get(keys)
        if obj is None:
            obj = super().__new__(cls, **kwargs)
            cls._cache[keys] = obj
        return obj

    @classmethod
    def get_all_cached_objects(cls) -> list[Self]:
        return [i for i in cls._cache.values()]


@dataclasses.dataclass
class RegionCode(CachableData):
    """
    Represents a region code in CN-Stats
    """

    reg_code: str = None
    name: str | None = None
    parent: Self | None = None
    is_deleted: bool = False

    @staticmethod
    def __new__(cls,
                reg_code: str,
                parent=None,
                is_deleted: bool = False,
                **kwargs):
        """
        Create or get an existing metric code object (depends on the metric_code, and db_code
        :param reg_code: the code of the region
        :param parent: the parent region, if has
        :param is_deleted: is the data not available in the source or not
        :param kwargs: other properties of the code: name, english_name,
            created_time, last_updated_time
        """
        if reg_code is None:
            raise ValueError('reg_code %s must not be None.' % (reg_code,))
        return super().__new__(cls, (reg_code,), **kwargs)

    def __init__(self,
                 reg_code: str,
                 parent: Self | str | None = None,
                 is_deleted: bool = False,
                 **kwargs) -> None:
        """
        Init the metric code
        :param reg_code: The code defined in CN-Stats
        :param parent: The parent region object or the reg_code of the parent region.
            It could be None if it is a top level metric.
        :param is_deleted: is the data not available in the source or not
        :param kwargs: other properties of the code: name, english_name,
            created_time, last_updated_time
        """
        super().__init__(**kwargs)

        self._reg_code = reg_code
        if isinstance(parent, str):
            parent = RegionCode(parent, None)
        if not hasattr(self, '_parent'):
            self._parent = parent
        elif self._parent is None and parent is not None:
            self._parent = parent

        name = kwargs.get('name')
        if not hasattr(self, '_name'):
            self._name = name
        elif self._name is None and name is not None:
            self._name = name

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
        return self._reg_code == __value._reg_code

    def __hash__(self, *args, **kwargs) -> int:
        """
        Get hash code of the metric
        :param args:
        :param kwargs:
        :return: Returns the hash code based on the metric_code, and db_code.
        """
        return self._reg_code.__hash__()

    def __repr__(self) -> str:
        """
        Get the string of the object
        :return: Returns the string of the object
        """
        return (f'RegionCode('
                f'reg_code={self.reg_code}, '
                f'name={self.name}, '
                f'parent_metric_code={None if self.parent is None else self.parent.reg_code}, '
                f'is_deleted={self.is_deleted}'
                f'{super().__repr__()})')

    @classmethod
    def extra_attribute_names(cls) -> set[str]:
        return {'english_name'}

    @classmethod
    def add_or_update(cls, lst: list[Self]) -> int:
        """
        Insert or update the data to database.
        :param lst: The list of region codes
        :return: The record count are saved
        """

        sql = """        
INSERT INTO cn_stats_region_codes AS t (
    region_code, 
    name, 
    english_name,
    parent_region_code, 
    created_time, 
    last_updated_time) 
VALUES(%s, %s, %s, %s, now(), now()) 
ON CONFLICT(region_code) 
DO UPDATE SET 
    name = EXCLUDED.name, 
    english_name = EXCLUDED.english_name,
    parent_region_code = EXCLUDED.parent_region_code, 
    last_updated_time = EXCLUDED.last_updated_time 
WHERE t.name <> EXCLUDED.name 
    OR t.english_name <> EXCLUDED.english_name 
    OR t.parent_region_code <> EXCLUDED.parent_region_code;
        """
        with data.dao.StatsRawDataDao.get_conn() as conn:
            with conn.cursor() as cursor:
                data = [(
                    i.reg_code,
                    i.name,
                    i.english_name,
                    (None if i.parent is None else i.parent.reg_code)) for i in lst]
                cursor.executemany(sql, data)
                return cursor.rowcount

    @classmethod
    def delete(cls, lst: list[Self]) -> int:
        """
        Delete the data from database
        :param lst: the list of region codes
        :return: The record count are deleted
        """

        sql = 'DELETE FROM cn_stats_region_codes WHERE region_code = %s;'

        with data.dao.StatsRawDataDao.get_conn() as conn:
            with conn.cursor() as cursor:
                data = [(i.reg_code,) for i in lst]
                cursor.executemany(sql, data)
                return cursor.rowcount

    @classmethod
    def get(cls, reg_code: str) -> Self:
        """
        Get metric code from DB
        :param reg_code: code of the region
        :return: Returns the region code object if found, otherwise returns None
        """

        sql = """
SELECT 
    region_code, 
    name, 
    english_name, 
    parent_region_code, 
    created_time, 
    last_updated_time 
FROM cn_stats_region_codes 
WHERE region_code = %s;
        """

        with data.dao.StatsRawDataDao.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (reg_code,))
                data = [
                    RegionCode(
                        reg_code=i[0],
                        parent=i[3],
                        kwargs={
                            'name': i[1],
                            'english_name': i[2],
                            'created_time': i[4],
                            'last_updated_time': i[5]
                        })
                    for i in cursor.fetchall()]
                if len(data) == 0:
                    return None
                else:
                    return data[0]

    @classmethod
    def list(cls, reg_code: str | None = None) -> list[Self]:
        """
        Get regions via db code and region code and its descendants
        :param reg_code:  Specific the region code or None for metrics
        :return: Returns the regions and its descendants
        """

        sql = """
WITH RECURSIVE cte_regions (
    region_code, 
    name, 
    english_name,
    parent_region_code, 
    created_time, 
    last_updated_time)
AS(
    SELECT 
        region_code, 
        name, 
        english_name, 
        parent_region_code, 
        created_time, 
        last_updated_time 
    FROM cn_stats_region_codes 
    WHERE (%s IS NULL OR region_code = %s)
    UNION
    SELECT 
        c.region_code, 
        c.name, 
        c.english_name, 
        c.parent_region_code, 
        c.created_time, 
        c.last_updated_time 
    FROM cn_stats_region_codes c
    INNER JOIN cte_regions r ON c.region_code = r.parent_region_code 
) 
SELECT * FROM cte_regions;        
        """
        with data.dao.StatsRawDataDao.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (reg_code, reg_code))
                data = [RegionCode(i[0], i[3], kwargs={
                    'name': i[1],
                    'english_name': i[2],
                    'created_time': i[4],
                    'last_updated_time': i[5]
                }) for i in cursor.fetchall()]
                return data


@dataclasses.dataclass
class MetricCode(CachableData):
    """
    Represents a metric in CN-Stats
    """

    metric_code: str = None
    db_code: DbCode = None
    name: str | None = None
    explanation: str | None = None
    memo: str | None = None
    unit: str | None = None
    parent: Self | None = None
    is_deleted: bool = False

    @staticmethod
    def __new__(cls,
                metric_code: str,
                db_code: DbCode,
                name: str | None,
                parent=None,
                is_deleted: bool = False,
                **kwargs: dict[str, Any]):
        """
        Create or get an existing metric code object (depends on the metric_code, and db_code
        :param metric_code: the code of the metric
        :param db_code: the metric belongs to which DbCode
        :param parent: the parent metric, if has
        :param is_deleted: is the data not available in the source or not
        :param kwargs: other properties of the code: name, english_name, explanation, memo,
            created_time, last_updated_time
        """
        if metric_code is None or db_code is None:
            raise ValueError('metric_code %s or db_cde %s must not be None.' % (metric_code, db_code))
        return super().__new__(cls, (metric_code, db_code), **kwargs)

    def __init__(self,
                 metric_code: str,
                 db_code: DbCode,
                 name: str | None,
                 parent: Self | str | None = None,
                 is_deleted: bool = False,
                 **kwargs) -> None:
        """
        Init the metric code
        :param metric_code: The code defined in CN-Stats
        :param db_code: The metric belongs to which DbCode
        :param parent: The parent metric object or the metric_code of the parent metric.
            It could be None if it is a top level metric.
        :param is_deleted: is the data not available in the source or not
        :param kwargs: other properties of the code: name, english_name, explanation, memo,
            created_time, last_updated_time
        """

        super().__init__(**kwargs)

        self._metric_code = metric_code
        self._db_code = db_code
        if isinstance(parent, str):
            parent = MetricCode(parent, db_code, None, None)
        if self.parent is None and parent is not None:
            self.parent = parent

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
        return self._metric_code == __value._metric_code \
            and self._db_code == __value._db_code

    def __hash__(self, *args, **kwargs) -> int:
        """
        Get hash code of the metric
        :param args:
        :param kwargs:
        :return: Returns the hash code based on the metric_code, and db_code.
        """
        return (self._metric_code, self._db_code).__hash__()

    def __repr__(self) -> str:
        """
        Get the string of the object
        :return: Returns the string of the object
        """
        return (f'MetricCode('
                f'metric_code={self.metric_code}, '
                f'db_code={self.db_code.code}, '
                f'name={self.name}, '
                f'explanation={self.explanation}, '
                f'memo={self.memo}, '
                f'unit={self.unit}, '
                f'parent_metric_code={None if self.parent is None else self.parent.metric_code}), '
                f'is_deleted={self.is_deleted}'
                f'{super().__repr__()})')

    @classmethod
    def extra_attribute_names(cls) -> set[str]:
        return {'english_name', 'english_explanation', 'english_memo', 'english_unit'}

    @classmethod
    def add_or_update(cls, lst: list[Self]) -> int:
        """
        Insert or update the data to database.
        :param lst: The list of metric codes
        :return: The record count are saved
        """

        sql = """        
INSERT INTO cn_stats_metric_codes AS t (
    metric_code, 
    db_code, 
    name, 
    english_name,
    explanation,
    memo,
    parent_metric_code, 
    created_time, 
    last_updated_time) 
VALUES(%s, %s, %s, %s, %s, %s, %s, now(), now()) 
ON CONFLICT(metric_code, db_code) 
DO UPDATE SET 
    name = EXCLUDED.name, 
    english_name = EXCLUDED.english_name,
    explanation = EXCLUDED.explanation,
    memo = EXCLUDED.memo
    parent_metric_code = EXCLUDED.parent_metric_code, 
    last_updated_time = EXCLUDED.last_updated_time 
WHERE t.name <> EXCLUDED.name 
    OR t.english_name <> EXCLUDED.english_name 
    OR t.explanation <> EXCLUDED.explanation 
    OR t.memo <> EXCLUDED.memo 
    OR t.parent_metric_code <> EXCLUDED.parent_metric_code;
        """
        with data.dao.StatsRawDataDao.get_conn() as conn:
            with conn.cursor() as cursor:
                data = [(
                    i.metric_code,
                    i.db_code.code,
                    i.name,
                    i.english_name,
                    i.explanation,
                    i.memo,
                    (None if i.parent is None else i.parent.metric_code)) for i in lst]
                cursor.executemany(sql, data)
                return cursor.rowcount

    @classmethod
    def delete(cls, lst: list[Self]) -> int:
        """
        Delete the data from database
        :param lst: the list of metric codes
        :return: The record count are deleted
        """

        sql = 'DELETE FROM cn_stats_metric_codes WHERE metric_code = %s AND db_code = %s;'

        with data.dao.StatsRawDataDao.get_conn() as conn:
            with conn.cursor() as cursor:
                data = [(i.metric_code, i.db_code.code) for i in lst]
                cursor.executemany(sql, data)
                return cursor.rowcount

    @classmethod
    def get(cls, metric_code: str, db_code: DbCode) -> Self:
        """
        Get metric code from DB
        :param metric_code: code of the metric
        :param db_code: db code of the metric
        :return: Returns the metric code object if found, otherwise returns None
        """

        sql = """
SELECT 
    metric_code, 
    db_code, 
    name, 
    english_name, 
    explanation, 
    memo, 
    parent_metric_code, 
    created_time, 
    last_updated_time 
FROM cn_stats_metric_codes 
WHERE metric_code = %s AND db_code = %s;
        """

        with data.dao.StatsRawDataDao.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (metric_code, db_code.code))
                data = [
                    MetricCode(
                        metric_code=i[0],
                        db_code=DbCode.get_code(i[1]),
                        name=i[2],
                        parent=i[6],
                        explanation=i[4],
                        memo=i[5],
                        english_name=i[3],
                        created_time=i[7],
                        last_updated_time=i[8])
                    for i in cursor.fetchall()]
                if len(data) == 0:
                    return None
                else:
                    return data[0]

    @classmethod
    def list(cls, db_code: DbCode | None = None, metric_code: str | None = None) -> list[Self]:
        """
        Get metrics via db code and metric code and its descendants
        :param db_code: Specific the db code or None for all db codes
        :param metric_code:  Specific the metric code or None for metrics
        :return: Returns the metrics and its descendants
        """

        sql = """
WITH RECURSIVE cte_metrics (
    metric_code, 
    db_code, 
    name, 
    english_name,
    explanation, 
    memo, 
    parent_metric_code, 
    created_time, 
    last_updated_time)
AS(
    SELECT 
        metric_code, 
        db_code, 
        name, 
        english_name, 
        explanation, 
        memo, 
        parent_metric_code, 
        created_time, 
        last_updated_time 
    FROM cn_stats_metric_codes 
    WHERE (%s IS NULL OR metric_code = %s) AND (%s IS NULL OR db_code = %s)
    UNION
    SELECT 
        c.metric_code, 
        c.db_code, 
        c.name, 
        c.english_name, 
        c.explanation, 
        c.memo, 
        c.parent_metric_code, 
        c.created_time, 
        c.last_updated_time 
    FROM cn_stats_metric_codes c
    INNER JOIN cte_metrics r ON c.metric_code = r.parent_metric_code AND c.db_code = r.db_code
) 
SELECT * FROM cte_metrics;        
        """
        with data.dao.StatsRawDataDao.get_conn() as conn:
            with conn.cursor() as cursor:
                dbcode = None if db_code is None else db_code.code
                cursor.execute(sql, (metric_code, metric_code, dbcode, dbcode))
                data = [MetricCode(
                    metric_code=i[0],
                    db_code=DbCode.get_code(i[1]),
                    name=i[2],
                    parent=i[6],
                    explanation=i[4],
                    memo=i[5],
                    english_name=i[3],
                    created_time=i[7],
                    last_updated_time=i[8]) for i in cursor.fetchall()]
                return data

    @staticmethod
    def import_metrics(db_code: DbCode | None = None) -> None:
        """
        Imports metric codes from getTree API to database
        :param db_code: The db_code of the metrics. If it's None, means to import all metric codes
        :return: None
        """
        db_codes: list[DbCode] = list(DbCode) if db_code is None else [db_code]
        for code in db_codes:
            print(f'Starting to download metric codes under {code.code} - {code.name}.')
            lst = data.dao.StatsRawDataDao.download_metrics(code)
            print(f'Retrieved {len(lst)} codes.')
            if len(lst) > 0:
                MetricCode.add_or_update(lst)
                print('Saved the codes.')


@dataclasses.dataclass
class MetricData(ExpandableData):
    """
    Represents the data of a particular metric
    """
    metric_code: MetricCode = None
    reg_code: str = None
    date_num: int = None
    metric_value: float = None
    is_deleted: bool = False

    def __init__(self,
                 metric_code: MetricCode,
                 data_num: int,
                 reg_code: str | None,
                 metric_value: float,
                 is_deleted: bool = False,
                 **kwargs) -> None:
        """
        Init the metric data object.
        :param metric_code: the metric code of the data
        :param data_num: the date of the metric data
        :param reg_code: the code of the region the data belongs to
        :param metric_value: the value of the data
        :param is_deleted: is the data not available in the source or not
        :param kwargs:
        """

        super().__init__(**kwargs)

        self.metric_code = metric_code
        self.date_num = data_num
        self.reg_code= reg_code
        self.metric_value = metric_value
        self.is_deleted = is_deleted

        pass

    @classmethod
    def extra_attribute_names(cls) -> set[str]:
        return {'metric_str_value'}

    @classmethod
    def add_or_update(cls, lst: list[Self]) -> int:
        """
        Insert or update the data to database.
        :param lst: The list of metric data
        :return: The record count are saved
        """

        sql = """        
INSERT INTO cn_stats_metric_data AS t (
    metric_code, 
    db_code, 
    reg_code,
    date_num, 
    metric_value, 
    created_time, 
    last_updated_time) 
VALUES(%s, %s, %s, %s, %s, now(), now()) 
ON CONFLICT(metric_code, db_code, reg_code, date_num) 
DO UPDATE SET 
    metric_value = EXCLUDED.metric_value, 
    last_updated_time = EXCLUDED.last_updated_time 
WHERE t.metric_value <> EXCLUDED.metric_value;
        """
        with data.dao.StatsRawDataDao.get_conn() as conn:
            with conn.cursor() as cursor:
                data = [(
                    i.metric_code.metric_code,
                    i.metric_code.db_code.code,
                    i.date_num,
                    i.metric_value) for i in lst]
                cursor.executemany(sql, data)
                return cursor.rowcount

    @classmethod
    def delete(cls, lst: list[Self]) -> int:
        """
        Delete the data from database
        :param lst: the list of metric data
        :return: The record count are deleted
        """

        sql = """
DELETE FROM cn_stats_metric_data 
WHERE metric_code = %s 
    AND db_code = %s 
    AND ((reg_code IS NULL AND %s IS NULL) OR reg_code = %s) 
    AND date_num = %s;
        """

        with data.dao.StatsRawDataDao.get_conn() as conn:
            with conn.cursor() as cursor:
                data = [(i.metric_code.metric_code, i.metric_code.db_code.code, i.reg_code, i.date_num) for i in lst]
                cursor.executemany(sql, data)
                return cursor.rowcount

    @classmethod
    def list(cls, db_code: DbCode | None = None, metric_code: str | None = None) -> list[Self]:
        """
        Get metric data of the metric code or its descendants by given db code and metric code
        :param db_code: Specific the db code or None for all db codes
        :param metric_code:  Specific the metric code or None for metrics
        :return: Returns the metric data for the specified metric code or its descendants
        """

        sql = """
WITH RECURSIVE cte_metrics (metric_code, db_code, name, parent_metric_code, created_time, last_updated_time)
AS(
    SELECT metric_code, db_code, name, parent_metric_code, created_time, last_updated_time 
    FROM cn_stats_metric_codes 
    WHERE (%s IS NULL OR metric_code = %s) AND (%s IS NULL OR db_code = %s)
    UNION
    SELECT c.metric_code, c.db_code, c.name, c.parent_metric_code, c.created_time, c.last_updated_time 
    FROM cn_stats_metric_codes c
    INNER JOIN cte_metrics r ON c.metric_code = r.parent_metric_code AND c.db_code = r.db_code
) 
SELECT d.metric_code, d.db_code, d.reg_code, d.date_num, d.metric_value
FROM cte_metrics c 
INNER JOIN cn_stats_metric_data d ON c.metric_code = d.metric_code AND c.db_code = d.db_code;        
        """
        with data.dao.StatsRawDataDao.get_conn() as conn:
            with conn.cursor() as cursor:
                dbcode = None if db_code is None else db_code.code
                cursor.execute(sql, (metric_code, metric_code, dbcode, dbcode))
                data = [
                    MetricData(
                        metric_code=MetricCode(code=i[0], db_code=DbCode.get_code(i[1])),
                        reg_code=i[2],
                        date_num=i[3],
                        metric_value=i[4])
                    for i in cursor.fetchall()]
                return data

    @staticmethod
    def import_metric_data(metric_code: MetricCode, since: datetime, to: datetime) -> None:
        """
        Imports metric data from API to database
        :param metric_code: The parent metric code of the metrics.
        :param since: date range to import
        :param to: date range to import
        :return: None
        """
        print(f'Starting to download metric data under {metric_code.metric_code} - {metric_code.name}.')
        lst: list[data.models.MetricData] = data.dao.StatsRawDataDao.download_metric_data(
            metric_code=metric_code,
            since=since,
            to=to)
        codes = set()
        for item in lst:
            codes.add(item.metric_code)
        print(f'Retrieved {len(lst)} records, {len(codes)} metrics.')
        if len(codes) > 0:
            MetricCode.add_or_update([i for i in codes])
            print('Save the metric code of the data.')
        if len(lst) > 0:
            MetricData.add_or_update(lst)
            print('Saved the metric data.')

#
# def main():
#     metric_code_1 = MetricCode('A01', DbCode.MACRO_ECONOMIC_MONTHLY, 'name 1', None)
#     metric_code_2 = MetricCode('A01', DbCode.MACRO_ECONOMIC_MONTHLY, 'name 2', None)
#     print(metric_code_1 == metric_code_2)
#     print(metric_code_1.name == metric_code_2.name)
#     #
#     # MetricCode.add_or_update([MetricCode('TEST01', DbCode.MACRO_ECONOMIC_ANNUAL, 'TEST Code', None)])
#     # print(MetricCode.get('TEST01', DbCode.MACRO_ECONOMIC_ANNUAL))
#     #
#     # MetricData.add_or_update([MetricData(MetricCode('TEST01', DbCode.MACRO_ECONOMIC_ANNUAL), 2024, 100.2)])
#     # print(len(MetricData.list(DbCode.MACRO_ECONOMIC_ANNUAL, 'TEST01')))
#     #
#     # MetricData.delete([MetricData(MetricCode('TEST01', DbCode.MACRO_ECONOMIC_ANNUAL), 2024, 100.2)])
#     # print(len(MetricData.list(DbCode.MACRO_ECONOMIC_ANNUAL, 'TEST01')))
#     #
#     # MetricCode.delete([MetricCode('TEST01', DbCode.MACRO_ECONOMIC_ANNUAL, None, None)])
#     # print(len(MetricCode.list()))
#     #
#     # MetricCode.import_metrics(DbCode.HMT_MONTHLY)
#     # MetricData.import_metric_data(
#     #     MetricCode(metric_code='A010301', db_code=DbCode.MACRO_ECONOMIC_ANNUAL),
#     #     datetime(year=2010, month=1, day=1),
#     #     datetime(year=2024, month=6, day=1))
#
#     pass
#
#
# if __name__ == '__main__':
#     main()
