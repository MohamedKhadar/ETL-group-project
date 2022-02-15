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
    query_create_table = """CREATE TABLE IF NOT EXISTS ORDERS(
    order_id varchar(255) NOT NULL PRIMARY KEY,
    date_time timestamp NOT NULL,
    store varchar(255) NOT NULL,
    total_spend numeric(20,2) NOT NULL,
    payment_method varchar(255) NOT NULL
);


CREATE TABLE IF NOT EXISTS PRODUCTS(
    product_id varchar(255) NOT NULL PRIMARY KEY,
    product_name varchar(255) NOT NULL
);


CREATE TABLE IF NOT EXISTS ORDER_PRODUCTS(
    order_id varchar(255) NOT NULL,
    constraint fk_orders foreign key(order_id) references orders(order_id),
    product_id varchar(255) NOT NULL,
    constraint fk_products foreign key(product_id) references products(product_id),
    price numeric(20,2) NOT NULL,
    quantity int NOT NULL
);

create temp table orders_staging (like orders);
create temp table products_staging (like products);
create temp table order_products_staging (like order_products);"""
    cursor.execute(query_create_table)
    query_insert_order = "INSERT INTO orders_staging (order_id, date_time, store, total_spend, payment_method) VALUES (%s, %s, %s, %s, %s)"
    query_insert_product = "INSERT INTO products_staging (product_id, product_name) VALUES (%s, %s);"
    query_insert_order_product = "INSERT INTO order_products_staging (order_id, product_id, price, quantity) VALUES (%s, %s, %s, %s);"
    query_orders_staging = "delete from orders_staging using orders where orders_staging.order_id = orders.order_id"
    query_products_staging = "delete from products_staging using products where products_staging.product_id = products.product_id"
    query_order_products_staging = "delete from order_products_staging using order_products where order_products_staging.order_id = order_products.order_id"
    LOGGER.info(f'inserting into order_staging table: {query_insert_order}')
    cursor.execute(query_insert_order, (order['order_id'], order['date_time'], order['store'], order['total_spend'], order['payment_method']))
    for item in order['items']:
        LOGGER.info(f'inserting into product_staging table: {query_insert_product}')
        cursor.execute(query_insert_product, (item['product_id'], item['product_name']))
        LOGGER.info(f'inserting into order_products_staging table: {query_insert_order_product}')
        cursor.execute(query_insert_order_product, (order['order_id'], item['product_id'], item['price'], item['quantity']))
    cursor.execute(query_orders_staging)
    cursor.execute(query_products_staging)
    cursor.execute(query_order_products_staging)
    insert_orders = "insert into orders select * from orders_staging"
    insert_products = "insert into products select * from products_staging"
    insert_order_products = "insert into order_products select * from order_products_staging"
    cursor.execute(insert_orders)
    cursor.execute(insert_products)
    cursor.execute(insert_order_products)
    query_drop_1 = "drop table order_products_staging"
    query_drop_2 = "drop table orders_staging, products_staging"
    cursor.execute(query_drop_1)
    cursor.execute(query_drop_2)
    conn.commit()
    conn.close()
