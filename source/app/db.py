from app.connect import get_connection
import boto3
import os
import logging

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


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

create temp table orders_staging (like orders);"""
    cursor.execute(query_create_table)
    query_insert_order = "INSERT INTO orders_staging (order_id, date_time, store, total_spend, payment_method) VALUES (%s, %s, %s, %s, %s)"
    query_orders_staging = "delete from orders_staging using orders where orders_staging.order_id = orders.order_id"
    LOGGER.info(f'inserting into order_staging table: {query_insert_order}')
    cursor.execute(query_insert_order, (order['order_id'], order['date_time'], order['store'], order['total_spend'], order['payment_method']))
    #for item in order:
    cursor.execute(query_orders_staging)
    insert_orders = "insert into orders select * from orders_staging"
    cursor.execute(insert_orders)
    query_drop_o = "drop table orders_staging"
    cursor.execute(query_drop_o)
    conn.commit()
    conn.close()


def insert_product(creds,products): 
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

create temp table products_staging (like products);"""
    cursor.execute(query_create_table)
    query_insert_product = "INSERT INTO products_staging (product_id, product_name) VALUES (%s, %s);"
    query_products_staging = "delete from products_staging using products where products_staging.product_id = products.product_id"
    LOGGER.info(f'inserting into products_staging table: {query_insert_product}')
    #for item in order:
    LOGGER.info(f'inserting into product_staging table: {query_insert_product}')
    cursor.execute(query_insert_product, (products['product_id'], products['product_name']))
    cursor.execute(query_products_staging)
    insert_products = "insert into products select * from products_staging"
    cursor.execute(insert_products)
    query_drop_products = "drop table products_staging"
    cursor.execute(query_drop_products)
    conn.commit()
    conn.close()


def insert_order_product(creds,order_products): 
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

create temp table order_products_staging (like order_products);"""
    cursor.execute(query_create_table)
    query_insert_order_product = "INSERT INTO order_products_staging (order_id, product_id, price, quantity) VALUES (%s, %s, %s, %s);"
    query_order_products_staging = "delete from order_products_staging using order_products where order_products_staging.order_id = order_products.order_id"
    #for item in order:
    LOGGER.info(f'inserting into order_products_staging table: {query_insert_order_product}')
    cursor.execute(query_insert_order_product, (order_products['order_id'], order_products['product_id'], order_products['price'], order_products['quantity']))
    cursor.execute(query_order_products_staging)
    insert_order_products = "insert into order_products select * from order_products_staging"
    cursor.execute(insert_order_products)
    query_drop_op = "drop table order_products_staging"
    cursor.execute(query_drop_op)
    conn.commit()
    conn.close()