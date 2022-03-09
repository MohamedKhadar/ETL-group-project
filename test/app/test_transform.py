import unittest

from source.app.transform import *


class TestTransform(unittest.TestCase):

    def test_dt(self):
        self.assertEqual(dt('25/08/2021 09:00'), '2021-08-25 09:00')
        self.assertEqual(dt('1/1/2021 09:00'), '2021-01-01 09:00')
        
        with self.assertRaises(ValueError): dt('32/12/2018 08:00')
        with self.assertRaises(ValueError): dt('01/01/2015 24:00')
    
    def test_id_from_string(self):
        self.assertTrue(len(id_from_string('Large Latte')) == 10)
        self.assertTrue(len(id_from_string('1')) == 10)
        self.assertTrue(len(id_from_string('.!-:')) == 10)
    
        self.assertTrue(type(id_from_string('Large Latte')) == str)
    
    def test_split_product_and_price(self):
        self.assertEqual(split_product_and_price('Large Latte - Hazelnut - 2.75'), 
                        {'product_id': id_from_string('Large Latte - Hazelnut'), 'product_name': 'Large Latte - Hazelnut', 'price': 2.75})
    
    def test_split_products_string_into_list_of_products(self):
        self.assertEqual(split_products_string_into_list_of_products('Large Latte, Medium Tea'), ['Large Latte', 'Medium Tea'])
    
    def test_map_list_of_products(self):
        self.assertEqual(map_list_of_products('Large Latte - Hazelnut - 2.75'),
                        [{'product_id': id_from_string('Large Latte - Hazelnut'), 'product_name': 'Large Latte - Hazelnut', 'price': 2.75, 'quantity': 1}])
        self.assertEqual(map_list_of_products('Large Latte - Hazelnut - 2.75, Large Latte - Hazelnut - 2.75, Medium Tea - 1.95'),
                        [{'product_id': id_from_string('Large Latte - Hazelnut'), 'product_name': 'Large Latte - Hazelnut', 'price': 2.75, 'quantity': 2},
                        {'product_id': id_from_string('Medium Tea'), 'product_name': 'Medium Tea', 'price': 1.95, 'quantity': 1}])
    
    def test_dt_to_num(self):
        self.assertEqual(dt_to_num('20/04/2001 19:57'), '010420')
    
    def test_prep_hash(self):
        self.assertEqual(prep_hash({'date_time': '25/08/2021 09:34', 'store': 'Chesterfield', 'customer': 'Brenda Park', 
                                    'items': 'Regular Latte - 2.15', 'total_spend': '2.15', 'payment_method': 'CASH', 'card_num': '', 'order_id': 3}),
                        '210825Chesterfield3')
    
    def test_map_values_based_on_key_name(self):
        order = {'date_time': '25/08/2021 09:34', 
                'store': 'Chesterfield', 'order_id': 78}
        self.assertEqual(map_values_based_on_key_name('order_id', 78, order),
                id_from_string(prep_hash(order)))
        self.assertEqual(map_values_based_on_key_name('date_time', '25/08/2021 09:34', order),
                dt('25/08/2021 09:34'))
        self.assertEqual(map_values_based_on_key_name('store', 'Chesterfield', order),
                    'Chesterfield')
    
    def test_map_list_of_orders(self):
        self.assertEqual(list(map_list_of_orders([{'date_time': '25/08/2021 09:00', 'store': 'Chesterfield', 'customer': 'Richard Copeland',
                                    'items': 'Regular Flavoured iced latte - Hazelnut - 2.75, Regular Flavoured iced latte - Hazelnut - 2.75, Regular Flavoured iced latte - Hazelnut - 2.75, Large Latte - 2.45',
                                    'total_spend': '5.2', 'payment_method': 'CARD', 'card_num': '5494173772652516', 'order_id': 1}])),
                        [{'items': [{'product_name': 'Regular Flavoured iced latte - Hazelnut', 'quantity': 3, 'price': 2.75, 'product_id': '2720354948'},
                                    {'product_name': 'Large Latte', 'quantity': 1, 'price': 2.45, 'product_id': '7209298201'}], 
                            'payment_method': 'CARD', 'date_time': '2021-08-25 09:00', 'store': 'Chesterfield', 'total_spend': '5.2',
                            'order_id': '5604638142'}])


if __name__ == '__main__':
    unittest.main()
