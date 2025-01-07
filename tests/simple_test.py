# tests/simple_test.py
import unittest

class SimpleTest(unittest.TestCase):
    def test_example(self):
        self.assertTrue(True)

if __name__ == '__main__':
    unittest.main()