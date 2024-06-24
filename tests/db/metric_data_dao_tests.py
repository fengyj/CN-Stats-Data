import unittest
from data.db_code import DbCode
from data.metric_data import MetricData
from data import MetricCode
from db.metric_data_dao import MetricDataDao


class MetricDataDaoTests(unittest.TestCase):

    def test_list_func(self) -> None:
        lst = MetricDataDao.list()
        self.assertIsNotNone(lst)
        self.assertTrue(len(lst) > 0)
        pass

    def test_list_add_or_update_func(self) -> None:

        metric_data = MetricData(
            metric_code=MetricCode('A01', DbCode.MACRO_ECONOMIC_ANNUAL, None),
            date_num=202412,
            region_code=None,
            metric_value=0.12
        )
        result = MetricDataDao.add_or_update([metric_data])
        self.assertEqual(1, result)
        metric_data.metric_value = 1.2
        result = MetricDataDao.add_or_update([metric_data])
        self.assertEqual(1, result)
        pass

    def test_delete_func(self) -> None:

        metric_data = MetricData(
            metric_code=MetricCode('A01', DbCode.MACRO_ECONOMIC_ANNUAL, None),
            date_num=202412,
            region_code=None,
            metric_value=0.12
        )
        result = MetricDataDao.add_or_update([metric_data])
        metric_data.is_deleted = True
        result = MetricDataDao.add_or_update([metric_data])
        self.assertEqual(0, result)
        result = MetricDataDao.delete([metric_data])
        self.assertEqual(1, result)
        metric_data.is_deleted = False
        result = MetricDataDao.delete([metric_data])
        self.assertEqual(0, result)
        result = MetricDataDao.add_or_update([metric_data])
        self.assertEqual(1, result)

        metric_data.is_deleted = True
        result = MetricDataDao.add_or_update([metric_data])
        pass


if __name__ == '__main__':
    unittest.main()
    pass
