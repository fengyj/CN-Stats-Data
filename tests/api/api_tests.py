from data.db_code import DbCode

from api.cn_stats_api import CNStatsAPIs
import unittest


class ApiTests(unittest.TestCase):

    def test_retrieve_metric_codes_func(self) -> None:
        result = CNStatsAPIs.retrieve_metric_codes(
            db_code=DbCode.MACRO_ECONOMIC_ANNUAL.code,
            metric_code='A01')
        self.assertTrue(result is not None)
        pass

    def test_retrieve_region_codes_func(self) -> None:
        result = CNStatsAPIs.retrieve_region_codes(
            db_code=DbCode.PROVINCE_ANNUAL.code,
            region_code='100000')
        self.assertTrue(result is not None)
        pass

    def test(self):
        result = CNStatsAPIs.test()
        self.assertIsNotNone(result)


if __name__ == '__main__':
    unittest.main()
    pass
