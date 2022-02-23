import csv


def extract(filename):
    orders = []
    field_names = ['date_time', 'store', 'customer', 'items', 'total_spend', 'payment_method', 'card_num']
    with open(filename, 'r') as csv_file:
        reader = csv.DictReader(csv_file, fieldnames = field_names)
        id = 1
        for row in reader:
            row['order_id'] = id
            id+=1
            orders.append(row)
        return orders

def extract_orders(filename):
    orders = []
    field_names = ['order_id', 'date_time', 'store', 'total_spend', 'payment_method']
    with open(filename, 'r') as csv_file:
        reader = csv.DictReader(csv_file, fieldnames = field_names)
        for row in reader:
            orders.append(row)
        return orders

def extract_products(filename):
    products = []
    field_names = ['product_id', 'product_name']
    with open(filename, 'r') as csv_file:
        reader = csv.DictReader(csv_file, fieldnames = field_names)
        for row in reader:
            products.append(row)
        return products

def extract_order_products(filename):
    order_products = []
    field_names = ['order_id','product_id', 'price', 'quantity']
    with open(filename, 'r') as csv_file:
        reader = csv.DictReader(csv_file, fieldnames = field_names)
        for row in reader:
            order_products.append(row)
        return order_products