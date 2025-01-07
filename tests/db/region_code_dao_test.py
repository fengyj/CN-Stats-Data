from datetime import datetime
import random
import unittest
from unittest.mock import MagicMock, patch

from cn_stats_util.models import Category
from cn_stats_data.db.region_code_dao import RegionCodeDao
from cn_stats_data.db.models import RegionCode


class RegionCodeDaoTests(unittest.TestCase):
    
    @patch('cn_stats_data.db.region_code_dao.db.get_conn')
    def test_list(self, mock_get_conn):
        # Mock the database connection and cursor
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        # Mock the cursor to return some data
        mock_cursor.fetchall.return_value = [
            ('110000', Category.CITY_ANNUAL.db_code, 'Beijing', 'Capital', ['110100', '110200'], {}, False, datetime(2024,1,1), datetime(2024,1,2), None),
            ('110100', Category.CITY_ANNUAL.db_code, 'Dongcheng', 'District', ['110101'], {}, False, datetime(2024,2,1), datetime(2024,2,1), '110000'),
            ('110200', Category.CITY_ANNUAL.db_code, 'Xicheng', 'District', None, {}, False, datetime(2024,1,1), datetime(2024,1,1), '110000'),
            ('110101', Category.CITY_ANNUAL.db_code, 'Street1', 'Street', None, {}, False, datetime(2024,1,1), datetime(2024,1,1), '110100'),
        ]

        # Call the list function
        result = RegionCodeDao.list(db_code=Category.CITY_ANNUAL)

        # Verify the result
        self.assertEqual(len(result), 4)
        
        # Verify root region (110000)
        root = next(filter(lambda x: x.code == '110000', result), None)
        self.assertIsNotNone(root)
        self.assertIsNone(root.parent)
        self.assertEqual(root.code, '110000')
        self.assertEqual(len(root.children), 2)
        self.assertEqual(root.children[0].code, '110100')
        self.assertEqual(root.children[1].code, '110200')

        # Verify first district (110100)
        district1 = next(filter(lambda x: x.code == '110100', result), None)
        self.assertIsNotNone(district1)
        self.assertEqual(district1.parent.code, '110000')
        self.assertEqual(len(district1.children), 1)
        self.assertEqual(district1.children[0].code, '110101')

        # Verify second district (110200)
        district2 = next(filter(lambda x: x.code == '110200', result), None)
        self.assertIsNotNone(district2)
        self.assertEqual(district2.parent.code, '110000')
        self.assertIsNone(district2.children)

        # Verify street (110101)
        street = next(filter(lambda x: x.code == '110101', result), None)
        self.assertIsNotNone(street)
        self.assertEqual(street.parent.code, '110100')
        self.assertIsNone(street.children)

    def test_get_func(self) -> None:
        region_code = RegionCodeDao.get('00', Category.PROVINCIAL_ANNUAL)
        self.assertIsNotNone(region_code)
        self.assertIsNone(region_code.parent)
        self.assertTrue(region_code.is_parent)


    def test_list_func(self) -> None:
        lst = RegionCodeDao.list(db_code=Category.PROVINCIAL_ANNUAL)
        self.assertIsNotNone(lst)
        self.assertGreater(len(lst), 0)
        region_code = next(filter(lambda x: x.code == '00', lst), None)
        self.assertIsNotNone(region_code)
        self.assertIsNone(region_code.parent)
        self.assertTrue(region_code.is_parent)
        self.assertGreater(len(region_code.children), 0)


    def test_list_add_or_update_func(self) -> None:

        region_code = RegionCodeDao.get('00', Category.PROVINCIAL_ANNUAL)
        eng_name = region_code.extra_attributes.get('english_name')
        region_code.extra_attributes['english_name'] = f'new name {random.random()}'
        result = RegionCodeDao.add_or_update([region_code])
        self.assertEqual(1, result)
        region_code.extra_attributes['english_name'] = eng_name
        result = RegionCodeDao.add_or_update([region_code])
        self.assertEqual(1, result)


    def test_delete_func(self) -> None:

        region_code = RegionCodeDao.get('00', Category.PROVINCIAL_ANNUAL)
        region_code.is_deleted = True
        result = RegionCodeDao.add_or_update([region_code])
        self.assertEqual(0, result)
        result = RegionCodeDao.delete([region_code])
        self.assertEqual(1, result)
        region_code.is_deleted = False
        result = RegionCodeDao.delete([region_code])
        self.assertEqual(0, result)
        result = RegionCodeDao.add_or_update([region_code])
        self.assertEqual(1, result)


if __name__ == '__main__':
    unittest.main()

