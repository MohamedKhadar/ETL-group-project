import unittest

from app.extract import *



class TestCsv(unittest.TestCase):

    def test_extract(self):
        self.assertEqual(extract('test/app/example_data.csv'),
                        [{'date_time': '25/08/2021 09:08', 'store': 'Chesterfield', 'customer': 'Michael Sparrow',
                        'items': 'Large Latte - 2.45, Large Latte - 2.45, Regular Latte - 2.15, Large Latte - 2.45', 'total_spend': '4.6',
                        'payment_method': 'CASH', 'card_num': '', 'order_id': 1}])


if __name__ == '__main__':
    unittest.main()

