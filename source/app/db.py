from app.connect import get_connection
import boto3
import os
import logging

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
# def create_orders_table():
#     conn = get_connection()
#     cursor = conn.cursor()
#     query = '''CREATE TABLE IF NOT EXISTS ORDERS(
#         order_id varchar(255) NOT NULL PRIMARY KEY,
#         date_time timestamp NOT NULL,
#         store varchar(255) NOT NULL,
#         total_spend numeric(20,2) NOT NULL,
#         payment_method varchar(255) NOT NULL
#     );'''
#     cursor.execute(query)
#     conn.commit()
#     conn.close()


# def create_order_products_table():
#     conn = get_connection()
#     cursor = conn.cursor()
#     query = '''CREATE TABLE IF NOT EXISTS ORDER_PRODUCTS(
#         order_id varchar(255) NOT NULL,
#         constraint fk_orders foreign key(order_id) references orders(order_id),
#         product_id varchar(255) NOT NULL,
#         constraint fk_products foreign key(product_id) references products(product_id),
#         price numeric(20,2) NOT NULL,
#         quantity int NOT NULL
#     );'''
#     cursor.execute(query)
#     conn.commit()
#     conn.close()


# def create_products_table():
#     conn = get_connection()
#     cursor = conn.cursor()
#     query = '''CREATE TABLE IF NOT EXISTS PRODUCTS(
#         product_id varchar(255) NOT NULL PRIMARY KEY,
#         product_name varchar(255) NOT NULL
#     );'''
#     cursor.execute(query)
#     conn.commit()
#     conn.close()
def get_ssm_parameters_under_path(path: str) -> dict:
    ssm_client = boto3.client("ssm", region_name="eu-west-1")
    response = ssm_client.get_parameters_by_path(
        Path=path,
        Recursive=True,
        WithDecryption=True
    )
    formatted_response = {os.path.basename(x["Name"]):x["Value"] for x in response["Parameters"]}
    return formatted_response


def insert_order(creds,order): 
    conn = get_connection(creds)
    cursor = conn.cursor() 
    query_insert_order = "INSERT INTO orders (order_id, date_time, store, total_spend, payment_method) VALUES (%s, %s, %s, %s, %s)"
    query_insert_product = "INSERT INTO products (product_id, product_name) VALUES (%s, %s);"
    query_insert_order_product = "INSERT INTO order_products (order_id, product_id, price, quantity) VALUES (%s, %s, %s, %s);"
    LOGGER.info(f'inserting into order table: {query_insert_order}')
    cursor.execute(query_insert_order, (order['order_id'], order['date_time'], order['store'], order['total_spend'], order['payment_method']))
    for item in order['items']:
        LOGGER.info(f'inserting into product table: {query_insert_product}')
        cursor.execute(query_insert_product, (item['product_id'], item['product_name']))
        LOGGER.info(f'inserting into order_products table: {query_insert_order_product}')
        cursor.execute(query_insert_order_product, (order['order_id'], item['product_id'], item['price'], item['quantity']))
    conn.commit()
    conn.close()
