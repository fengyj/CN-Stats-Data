import unittest

from cn_stats_data.db.metric_data_dao import MetricDataDao

from cn_stats_data.db.models import MetricCode, RegionCode, MetricHistoricalData
from cn_stats_util.models import Category

class MetricDataDaoTests(unittest.TestCase):

    def test_list_func(self) -> None:
        lst = MetricDataDao.list()
        self.assertIsNotNone(lst)
        self.assertTrue(len(lst) > 0)


    def test_list_add_or_update_func(self) -> None:

        data = MetricHistoricalData(
            metric_code='A01',
            db_code= Category.MACRO_ANNUAL.db_code,
            period=202412,
            region_code=None,
            data=0.12,
            has_data=True
        )
        result = MetricDataDao.add_or_update([data])
        self.assertEqual(1, result)
        data.data = 1.2
        result = MetricDataDao.add_or_update([data])
        self.assertEqual(1, result)


    def test_delete_func(self) -> None:

        data = MetricHistoricalData(
            metric_code='A01',
            db_code= Category.MACRO_ANNUAL.db_code,
            period=202312,
            region_code=None,
            data=0.12,
            has_data=True
        )
        MetricDataDao.add_or_update([data])
        data.is_deleted = True
        result = MetricDataDao.add_or_update([data])
        self.assertEqual(0, result)
        result = MetricDataDao.delete([data])
        self.assertEqual(1, result)
        data.is_deleted = False
        result = MetricDataDao.delete([data])
        self.assertEqual(0, result)
        result = MetricDataDao.add_or_update([data])
        self.assertEqual(1, result)

        data.is_deleted = True
        MetricDataDao.add_or_update([data])


if __name__ == '__main__':
    unittest.main()

