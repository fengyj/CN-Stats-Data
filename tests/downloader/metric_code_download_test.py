import unittest
from unittest.mock import patch, MagicMock
from cn_stats_data.db.models import MetricCodeDownloadCheckpoint
from cn_stats_data.downloader.metric_code_download import download_metric_codes
from cn_stats_util.models import Category, Metric

class TestDownloadMetricCodes(unittest.TestCase):

    @patch('cn_stats_data.downloader.metric_code_download.ChinaStatsDataApis')
    @patch('cn_stats_data.downloader.metric_code_download.MetricCodeDao')
    @patch('cn_stats_data.downloader.metric_code_download.ProcessDataDao')
    def test_download_all_metric_codes(self, mock_process_data_dao, mock_metric_code_dao, mock_apis):
        # Mock the API response
        mock_apis_instance = mock_apis.return_value
        mock_apis_instance.fetch_metrics.return_value = [
            Metric.of(Category.CITY_ANNUAL.db_code, "A01"), 
            Metric.of(Category.CITY_ANNUAL.db_code, "A02")]

        # Mock the database response
        mock_metric_code_dao.list.return_value = []

        # Mock the checkpoint
        mock_checkpoint = MagicMock()
        mock_process_data_dao.get_metric_code_download_checkpoint.return_value = mock_checkpoint

        # Call the function
        download_metric_codes()

        # Assertions
        mock_apis_instance.fetch_metrics.assert_called()
        mock_metric_code_dao.add_or_update.assert_called()
        mock_metric_code_dao.delete.assert_called()

    @patch('cn_stats_data.downloader.metric_code_download.ChinaStatsDataApis')
    @patch('cn_stats_data.downloader.metric_code_download.MetricCodeDao')
    @patch('cn_stats_data.downloader.metric_code_download.ProcessDataDao')
    def test_download_descendants_of_metric_code(self, mock_process_data_dao, mock_metric_code_dao, mock_apis):
        # Mock the API response
        mock_apis_instance = mock_apis.return_value
        mock_apis_instance.fetch_metrics.return_value = [Metric(code='A01'), Metric(code='A02')]

        # Mock the database response
        mock_metric_code_dao.list.return_value = []

        # Mock the checkpoint
        mock_checkpoint = MagicMock()
        mock_process_data_dao.get_metric_code_download_checkpoint.return_value = mock_checkpoint

        # Call the function
        download_metric_codes(db_code=Category.MACRO_MONTHLY.db_code, metric_code='A01')

        # Assertions
        mock_apis_instance.fetch_metrics.assert_called_with(Category.MACRO_MONTHLY, parent=Metric.of(Category.MACRO_MONTHLY.db_code, code='A01'), recursive_fetch=False)
        mock_metric_code_dao.add_or_update.assert_called()
        mock_metric_code_dao.delete.assert_called()

    @patch('cn_stats_data.downloader.metric_code_download.ChinaStatsDataApis')
    @patch('cn_stats_data.downloader.metric_code_download.MetricCodeDao')
    @patch('cn_stats_data.downloader.metric_code_download.ProcessDataDao')
    def test_handle_exception(self, mock_process_data_dao, mock_metric_code_dao, mock_apis):
        # Mock the API to raise an exception
        mock_apis_instance = mock_apis.return_value
        mock_apis_instance.fetch_metrics.side_effect = Exception('API error')

        # Mock the checkpoint
        mock_checkpoint = MagicMock()
        mock_process_data_dao.get_metric_code_download_checkpoint.return_value = mock_checkpoint

        # Call the function
        download_metric_codes()

        # Assertions
        mock_apis_instance.fetch_metrics.assert_called()
        mock_metric_code_dao.add_or_update.assert_not_called()
        mock_metric_code_dao.delete.assert_not_called()

if __name__ == '__main__':
    unittest.main()