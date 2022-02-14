import csv


def extract(filename):
    orders = []
    field_names = ['date_time', 'store', 'customer', 'items', 'total_spend', 'payment_method', 'card_num']
    with open(filename, 'r') as csv_file:
        reader = csv.DictReader(csv_file, fieldnames = field_names)
        for row in reader:
            orders.append(row)
    return orders


