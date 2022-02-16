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

