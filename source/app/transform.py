from hashlib import sha256
from uuid import uuid1
from datetime import datetime
import operator
import itertools


def dt(date):   #formats date&time to postgreSQL 'timestamp'
    return datetime.strptime(date, "%d/%m/%Y %H:%M").strftime("%Y-%m-%d %H:%M")


def id_from_string(input_string):   #creates unique hash for each product
    return str(int(sha256(input_string.encode('utf-8')).hexdigest(), 16))[:10]


def get_unique_id():    #creates a unique id for each order_id
    return str(uuid1())


def split_product_and_price(product_string):    #splits the 'items' into product_id, product_name, price 
    split = product_string.rsplit(' - ', 1)
    return {'product_id': id_from_string(split[0]), 'product_name': split[0], 'price': float(split[1])}


def split_products_string_into_list_of_products(products_string):   #splits unique products
    return products_string.split(', ')


def map_list_of_products(list_of_products):     #maps data to new list, implements transformations
    product_list = list(map(lambda product: split_product_and_price(product),
                    split_products_string_into_list_of_products(list_of_products)))
    # Sort by product_id so that grouping works
    product_list.sort(key=operator.itemgetter('product_id'))
    # get a list of dictionaries containing product_id and quantity
    grouped_list = [{key: len(list(group))} for key, group in
                    itertools.groupby(product_list, key=lambda k: k['product_id'])]
    # convert list of dictionaries into a dictionary with the key as the product_id and the value as the quantity
    grouped_dict = dict((key, d[key]) for d in grouped_list for key in d)
    # remove duplicates from the list of products
    product_set = list({v['product_id']: v for v in product_list}.values())
    # merge the quantity into the deduplicated list of products
    return list(map(lambda items: {
        key: (grouped_dict.get(items.get('product_id')) if key == 'quantity' else items[key])
        for key in (set(items.keys()).union({'quantity'}))}, product_set))


def map_values_based_on_key_name(key, value):   #applies date_time and uuid functions to respective keys
    if key == 'order_id':
        return get_unique_id()
    elif key == 'date_time':
        return dt(value)
    else:
        return value


def map_list_of_orders(list_of_orders):     #produces transformed list of data
    exclude_keys = {'customer', 'card_num'}
    extra_keys = {'order_id'}
    return map(lambda items: {
        key: (map_values_based_on_key_name(key, items.get(key)) if key != 'items' else map_list_of_products(items[key]))
        for key in (set(items.keys()).union(extra_keys) - exclude_keys)}, list_of_orders)
