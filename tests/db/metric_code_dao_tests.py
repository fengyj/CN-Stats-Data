import random
import unittest
from data.db_code import DbCode
from db.metric_code_dao import MetricCodeDao


class MetricCodeDaoTests(unittest.TestCase):

    def test_get_func(self) -> None:

        metric_code = MetricCodeDao.get('A01', DbCode.MACRO_ECONOMIC_ANNUAL)
        self.assertIsNotNone(metric_code)
        pass

    def test_list_func(self) -> None:

        lst = MetricCodeDao.list(db_code=DbCode.MACRO_ECONOMIC_ANNUAL)
        self.assertIsNotNone(lst)
        self.assertTrue(len(lst) > 0)
        pass

    def test_list_add_or_update_func(self) -> None:

        metric_code = MetricCodeDao.get('A01', DbCode.MACRO_ECONOMIC_ANNUAL)
        eng_name = metric_code.extra_attributes.get('english_name')
        metric_code.extra_attributes['english_name'] = f'new name {random.random()}'
        result = MetricCodeDao.add_or_update([metric_code])
        self.assertEqual(1, result)
        metric_code.extra_attributes['english_name'] = eng_name
        result = MetricCodeDao.add_or_update([metric_code])
        self.assertEqual(1, result)
        pass

    def test_delete_func(self) -> None:

        metric_code = MetricCodeDao.get('A01', DbCode.MACRO_ECONOMIC_ANNUAL)
        metric_code.is_deleted = True
        result = MetricCodeDao.add_or_update([metric_code])
        self.assertEqual(0, result)
        result = MetricCodeDao.delete([metric_code])
        self.assertEqual(1, result)
        metric_code.is_deleted = False
        result = MetricCodeDao.delete([metric_code])
        self.assertEqual(0, result)
        result = MetricCodeDao.add_or_update([metric_code])
        self.assertEqual(1, result)
        pass


if __name__ == '__main__':
    unittest.main()
    pass
