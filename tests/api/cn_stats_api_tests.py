import unittest

from cn_stats_data.api.cn_stats_api import CNStatsAPIs
from cn_stats_data.data.db_code import DbCode


class ApiTests(unittest.TestCase):

    def test_retrieve_metric_codes_func(self) -> None:
        result = CNStatsAPIs.retrieve_metric_codes(
            db_code=DbCode.MACRO_ECONOMIC_ANNUAL.code,
            metric_code='A01')
        self.assertIsNotNone(result)

    def test_retrieve_region_codes_func(self) -> None:
        result = CNStatsAPIs.retrieve_region_codes(
            db_code=DbCode.PROVINCE_ANNUAL.code,
            region_code='100000')
        self.assertIsNotNone(result)


if __name__ == '__main__':
    unittest.main()

