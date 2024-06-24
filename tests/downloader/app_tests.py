import unittest
from downloader.app import download_metric_codes, download_region_codes, download_metric_data
from data.db_code import DbCode
import logging


class AppTests(unittest.TestCase):

    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

    def test_metric_code_downloader(self) -> None:
        download_metric_codes(db_code=DbCode.MACRO_ECONOMIC_ANNUAL.code, metric_code=None)
        self.assertTrue(True)
        pass

    def test_region_code_downloader(self) -> None:
        download_region_codes(db_code=None)
        self.assertTrue(True)
        pass

    def test_metric_data_downloader(self) -> None:
        download_metric_data(
            years=['2019', '2020', '2021', '2022', '2023'],
            db_code=DbCode.MACRO_ECONOMIC_ANNUAL.code,
            metric_codes=['A020C01']
        )
        self.assertTrue(True)
        pass


if __name__ == '__main__':
    unittest.main()
    pass