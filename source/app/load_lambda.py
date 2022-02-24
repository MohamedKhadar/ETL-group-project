from logging import Logger
import requests
import os
import json
import boto3
import logging
import app.extract as extract
import app.transform as transfrom
import app.run as run
import app.connect as connect
import app.db as db

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)



def lambda_handler(event, context):
    LOGGER.info(event)
    filepath = '/tmp/output.csv'
    event_body = json.loads(event['Records'][0]['body'])
    LOGGER.info(event_body)
    s3_bucket = event_body['bucket_name']
    s3_object_key = event_body['bucket_key']
    data_type = event_body['data_type']
    LOGGER.info(f'triggered by file: {s3_object_key} in bucket: {s3_bucket}')
    s3 = boto3.client('s3')
    s3.download_file(s3_bucket, s3_object_key, filepath)
    
    
    creds = db.get_ssm_parameters_under_path('/team3/redshift')
    if data_type == 'orders':
        orders = extract.extract_orders(filepath)
        LOGGER.info(f'Extracted orders data, first row is {orders[0]}')
        for x in orders:
            LOGGER.info('inserting order into order table')
            db.insert_order(creds, x)
    elif data_type == 'products':
        products = extract.extract_products(filepath)
        LOGGER.info(f'Extracted products data, first row is {products[0]}')
        for y in products:
            LOGGER.info('inserting products into products table')
            db.insert_product(creds, y)
    elif data_type == 'order_products':
        order_products = extract.extract_order_products(filepath)
        LOGGER.info(f'Extracted order_products data, first row is {order_products[0]}')
        for z in order_products:
            LOGGER.info('inserting order_products into order_products table')
            db.insert_order_product(creds, z)
    else:
        print(f"invalid data type {data_type}!")