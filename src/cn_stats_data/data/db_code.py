from enum import Enum
from functools import cache
from typing import Self


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
    CITY_MONTHLY = 'csyd', '城市月度'
    HMT_ANNUAL = 'gatnd', '港澳台年度'
    HMT_MONTHLY = 'gatyd', '港澳台月度'
    GBL_ANNUAL = 'gjnd', '主要国家(地区)年度'
    GBL_MONTHLY = 'gjyd', '主要国家(地区)月度'
    JUE_MONTHLY = 'gjydsdj', '三大经济体月度'
    GBL_MARKET_MONTHLY = 'gjydsc', '国际市场月度商品价格'

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

    def __str__(self) -> str:
        return f'DbCode(code={self.code}, name={self.name})@{id(self)}'

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
        return self._code[-2:] == 'yd' or self._code in ['gjydsdj', 'gjydsc']

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
        return [
            cls.MACRO_ECONOMIC_ANNUAL,
            cls.PROVINCE_ANNUAL,
            cls.CITY_ANNUAL,
            cls.HMT_ANNUAL,
            cls.GBL_ANNUAL
        ]

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
        return [
            cls.MACRO_ECONOMIC_MONTHLY,
            cls.PROVINCE_MONTHLY,
            cls.CITY_MONTHLY,
            cls.HMT_MONTHLY,
            cls.GBL_MONTHLY,
            cls.JUE_MONTHLY,
            cls.GBL_MARKET_MONTHLY
        ]

    @classmethod
    @cache
    def get_region_codes(cls) -> list[Self]:
        return [
            cls.PROVINCE_ANNUAL,
            cls.PROVINCE_QUARTERLY,
            cls.CITY_ANNUAL,
            cls.CITY_MONTHLY,
            cls.HMT_ANNUAL,
            cls.HMT_MONTHLY,
            cls.GBL_ANNUAL,
            cls.GBL_MONTHLY,
            cls.GBL_MARKET_MONTHLY,
            cls.JUE_MONTHLY
        ]

    @classmethod
    @cache
    def get_global_codes(cls) -> list[Self]:
        return [
            cls.GBL_ANNUAL,
            cls.GBL_MONTHLY,
            cls.GBL_MARKET_MONTHLY,
            cls.JUE_MONTHLY
        ]

    @classmethod
    @cache
    def get_hk_mo_tw_codes(cls) -> list[Self]:
        return [
            cls.HMT_ANNUAL,
            cls.HMT_MONTHLY
        ]

    @classmethod
    @cache
    def get_code(cls, code: str) -> Self | None:
        """
        Get DbCode object via code
        :param code: the code of the DbCode
        :return: Returns the DbCode object if found, otherwise returns None
        """
        for e in DbCode:
            if e.code == code:
                return e
        return None


