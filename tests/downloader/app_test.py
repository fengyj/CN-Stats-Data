import logging
import unittest

from cn_stats_util.models import Category
from cn_stats_data.downloader.metric_code_download import download_metric_codes
from cn_stats_data.downloader.metric_data_download import download_metric_data
from cn_stats_data.downloader.region_code_download import download_region_codes


class AppTests(unittest.TestCase):

    def test_metric_code_downloader(self) -> None:
        download_metric_codes(db_code=None, metric_code=None)
        self.assertTrue(True)

    def test_region_code_downloader(self) -> None:
        download_region_codes(db_code=None, region_code=None)
        self.assertTrue(True)

    def test_metric_data_downloader(self) -> None:
        download_metric_data(
            years=[2015, 2016, 2017, 2018, 2019],
            db_code=Category.CITY_MONTHLY, # todo: remaining marco, and province
            metric_codes=None
        )
        self.assertTrue(True)

    def test_metric_data_downloader_2(self) -> None:
        # todo: macro monthly
        s = ''
        codes = s.split(',')
        download_metric_data(
            years=[str(i) for i in range(1995, 2020)],
            db_code=Category.MACRO_MONTHLY,
            metric_codes=codes
        )
        self.assertTrue(True)



if __name__ == '__main__':
    unittest.main()