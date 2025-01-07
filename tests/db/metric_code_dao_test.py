from datetime import datetime
import random
import unittest
from unittest.mock import MagicMock, patch

from cn_stats_util.models import Category
from cn_stats_data.db.metric_code_dao import MetricCodeDao


class MetricCodeDaoTests(unittest.TestCase):
    
    @patch('cn_stats_data.db.metric_code_dao.db.get_conn')
    def test_list(self, mock_get_conn):
        # Mock the database connection and cursor
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_conn.return_value.__enter__.return_value = mock_conn


        # Mock the cursor to return some data
        mock_cursor.fetchall.return_value = [
            ('A01', Category.CITY_ANNUAL.db_code, 'Metric 1', 'Explanation 1', 'Memo 1', 'Unit 1', None, {}, False, datetime(2024,1,1), datetime(2024,1,2)),
            ('A02', Category.CITY_ANNUAL.db_code, 'Metric 2', 'Explanation 2', 'Memo 2', 'Unit 2', 'A01', {}, False, datetime(2024,2,1), datetime(2024,2,1)),
            ('A03', Category.CITY_ANNUAL.db_code, 'Metric 3', 'Explanation 3', 'Memo 3', 'Unit 3', 'A01', {}, False, datetime(2024,1,1), datetime(2024,1,1)),
            ('A04', Category.CITY_ANNUAL.db_code, 'Metric 4', 'Explanation 4', 'Memo 4', None, 'A02', {}, False, datetime(2024,1,1), datetime(2024,1,1)),
        ]

        # Call the list function
        result = MetricCodeDao.list(db_code=Category.CITY_ANNUAL)

        # Verify the result
        self.assertEqual(len(result), 4)
        parent = next(filter(lambda x: x.code == 'A01', result), None)
        self.assertIsNotNone(parent)
        self.assertIsNone(parent.parent)
        self.assertEqual(parent.code, 'A01')
        self.assertEqual(len(parent.children), 2)
        self.assertEqual(parent.children[0].code, 'A02')
        self.assertEqual(parent.children[1].code, 'A03')

        a02 = next(filter(lambda x: x.code == 'A02', result), None)
        self.assertIsNotNone(a02)
        self.assertEqual(a02.parent.code, 'A01')
        self.assertEqual(len(a02.children), 1)
        self.assertEqual(a02.children[0].code, 'A04')

        a03 = next(filter(lambda x: x.code == 'A03', result), None)
        self.assertIsNotNone(a03)
        self.assertEqual(a03.parent.code, 'A01')
        self.assertFalse(a03.children)

        a04 = next(filter(lambda x: x.code == 'A04', result), None)
        self.assertIsNotNone(a04)
        self.assertEqual(a04.parent.code, 'A02')
        self.assertFalse(a04.children)


    def test_get_func(self) -> None:
        metric_code = MetricCodeDao.get('A01', Category.MACRO_ANNUAL)        
        self.assertIsNotNone(metric_code)
        self.assertIsNone(metric_code.parent)
        self.assertTrue(metric_code.is_parent)

    def test_list_func(self) -> None:
        lst = MetricCodeDao.list(db_code=Category.MACRO_ANNUAL)
        self.assertIsNotNone(lst)
        self.assertGreater(len(lst), 0)
        metric_code = next(filter(lambda x: x.code == 'A01', lst), None)      
        self.assertIsNotNone(metric_code)
        self.assertIsNone(metric_code.parent)
        self.assertTrue(metric_code.is_parent)
        self.assertGreater(len(metric_code.children), 0)

    def test_list_add_or_update_func(self) -> None:
        metric_code = MetricCodeDao.get('A01', Category.MACRO_ANNUAL)
        eng_name = metric_code.extra_attributes.get('english_name')
        metric_code.extra_attributes['english_name'] = f'new name {random.random()}'
        result = MetricCodeDao.add_or_update([metric_code])
        self.assertEqual(1, result)
        metric_code.extra_attributes['english_name'] = eng_name
        result = MetricCodeDao.add_or_update([metric_code])
        self.assertEqual(1, result)

    def test_delete_func(self) -> None:
        metric_code = MetricCodeDao.get('A01', Category.MACRO_ANNUAL)
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


if __name__ == '__main__':
    unittest.main()
