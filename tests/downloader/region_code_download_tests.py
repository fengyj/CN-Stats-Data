import unittest
from unittest.mock import patch, MagicMock
from cn_stats_data.db.models import RegionCodeDownloadCheckpoint
from cn_stats_data.downloader.region_code_download import download_region_codes
from cn_stats_util.models import Category, Region

class TestDownloadRegionCodes(unittest.TestCase):

    @patch('cn_stats_data.downloader.region_code_download.ChinaStatsDataApis')
    @patch('cn_stats_data.downloader.region_code_download.RegionCodeDao')
    @patch('cn_stats_data.downloader.region_code_download.ProcessDataDao')
    def test_download_all_region_codes(self, mock_process_data_dao, mock_region_code_dao, mock_apis):
        # Mock the API response
        mock_apis_instance = mock_apis.return_value
        mock_apis_instance.fetch_regions.return_value = [
            Region.of(Category.CITY_ANNUAL.db_code, "110000"), 
            Region.of(Category.CITY_ANNUAL.db_code, "120000")
        ]

        # Mock the database response
        mock_region_code_dao.list.return_value = []

        # Mock the checkpoint
        mock_checkpoint = MagicMock()
        mock_process_data_dao.get_region_code_download_checkpoint.return_value = mock_checkpoint

        # Call the function
        download_region_codes()

        # Assertions
        mock_apis_instance.fetch_regions.assert_called()
        mock_region_code_dao.add_or_update.assert_called()
        mock_region_code_dao.delete.assert_called()

    @patch('cn_stats_data.downloader.region_code_download.ChinaStatsDataApis')
    @patch('cn_stats_data.downloader.region_code_download.RegionCodeDao')
    @patch('cn_stats_data.downloader.region_code_download.ProcessDataDao')
    def test_download_descendants_of_region_code(self, mock_process_data_dao, mock_region_code_dao, mock_apis):
        # Mock the API response
        mock_apis_instance = mock_apis.return_value
        mock_apis_instance.fetch_regions.return_value = [
            Region.of(Category.CITY_ANNUAL.db_code, "110100"),
            Region.of(Category.CITY_ANNUAL.db_code, "110200")
        ]

        # Mock the database response
        mock_region_code_dao.list.return_value = []

        # Mock the checkpoint
        mock_checkpoint = MagicMock()
        mock_process_data_dao.get_region_code_download_checkpoint.return_value = mock_checkpoint

        # Call the function
        download_region_codes(db_code=Category.CITY_ANNUAL, region_code='110000')

        # Assertions
        mock_apis_instance.fetch_regions.assert_called_with(
            Category.CITY_ANNUAL, 
            parent=Region.of(Category.CITY_ANNUAL.db_code, code='110000'), 
            recursive_fetch=False
        )
        mock_region_code_dao.add_or_update.assert_called()
        mock_region_code_dao.delete.assert_called()

    @patch('cn_stats_data.downloader.region_code_download.ChinaStatsDataApis')
    @patch('cn_stats_data.downloader.region_code_download.RegionCodeDao')
    @patch('cn_stats_data.downloader.region_code_download.ProcessDataDao')
    def test_handle_exception(self, mock_process_data_dao, mock_region_code_dao, mock_apis):
        # Mock the API to raise an exception
        mock_apis_instance = mock_apis.return_value
        mock_apis_instance.fetch_regions.side_effect = Exception('API error')

        # Mock the checkpoint
        mock_checkpoint = MagicMock()
        mock_process_data_dao.get_region_code_download_checkpoint.return_value = mock_checkpoint

        # Call the function
        download_region_codes()

        # Assertions
        mock_apis_instance.fetch_regions.assert_called()
        mock_region_code_dao.add_or_update.assert_not_called()
        mock_region_code_dao.delete.assert_not_called()

if __name__ == '__main__':
    unittest.main()