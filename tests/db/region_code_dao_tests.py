import random
import unittest
from data.db_code import DbCode
from db.region_code_dao import RegionCodeDao


class RegionCodeDaoTests(unittest.TestCase):

    def test_get_func(self) -> None:
        region_code = RegionCodeDao.get('00', DbCode.PROVINCE_ANNUAL)
        self.assertIsNotNone(region_code)
        pass

    def test_list_func(self) -> None:
        lst = RegionCodeDao.list(db_code=DbCode.PROVINCE_ANNUAL)
        self.assertIsNotNone(lst)
        self.assertTrue(len(lst) > 0)
        pass

    def test_list_add_or_update_func(self) -> None:

        region_code = RegionCodeDao.get('00', DbCode.PROVINCE_ANNUAL)
        eng_name = region_code.extra_attributes.get('english_name')
        region_code.extra_attributes['english_name'] = f'new name {random.random()}'
        result = RegionCodeDao.add_or_update([region_code])
        self.assertEqual(1, result)
        region_code.extra_attributes['english_name'] = eng_name
        result = RegionCodeDao.add_or_update([region_code])
        self.assertEqual(1, result)
        pass

    def test_delete_func(self) -> None:

        region_code = RegionCodeDao.get('00', DbCode.PROVINCE_ANNUAL)
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
        pass


if __name__ == '__main__':
    unittest.main()
    pass
