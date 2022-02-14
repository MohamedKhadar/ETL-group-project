# from db import create_orders_table, create_order_products_table, create_products_table, insert_order
# from extract import extract
from app.transform import map_list_of_orders


filepath = '/tmp/somefile.csv'


def data_structure(data):
    # data = extract(filepath)
    return list(map_list_of_orders(data))
    # print(orders)
    # create_orders_table()
    # create_products_table()
    # create_order_products_table()

    # for order in orders:
    #     insert_order(order)


# data_structure()


