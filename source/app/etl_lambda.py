from csv import DictWriter
from itertools import product
from logging import Logger
import queue
import requests
import os
import csv
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
    filepath = '/tmp/somefile.csv'
    s3_event = event['Records'][0]['s3']
    bucket_name = s3_event['bucket']['name']
    object_name = s3_event['object']['key']
    LOGGER.info(f'triggered by file: {object_name} in bucket: {bucket_name}')
    s3 = boto3.client('s3')
    s3.download_file(bucket_name, object_name, filepath)
    
    data = extract.extract(filepath)
    # print(data[0])
    
    transformed_data = run.data_structure(data)
    LOGGER.info(f"Transformed data: {transformed_data[0]}")

    #Take each transformed data sets
    #For each set push to s3 as csv
    #For each file put a message on sqs to trigger the next phase

    base_filename = get_filename_no_ex(object_name)

    orders_table = []
    products_table = []
    order_products_table = []
    
    for x in transformed_data:
        order = {
            'order_id': x['order_id'],
            'date_time': x['date_time'],
            'store': x['store'],
            'total_spend': x['total_spend'],
            'payment_method': x['payment_method']
        }

        orders_table.append(order)
    

    for item in transformed_data:
        for y in item['items']:
            product = {
            'product_id': y['product_id'],
            'product_name': y['product_name']
            }
            products_table.append(product)
            order_product = {
            'order_id': item['order_id'],
            'product_id': y['product_id'],
            'price': y['price'],
            'quantity': y['quantity']
            }
            order_products_table.append(order_product)
    
    orders = orders_table
    products = products_table
    order_products = order_products_table
    
    sqs = boto3.client('sqs')

    send_file(s3, sqs, orders, 'orders', base_filename + 'orders.csv')
    send_file(s3, sqs, products, 'products', base_filename + 'products.csv')
    send_file(s3, sqs, order_products, 'order_products', base_filename + 'order_products.csv')


def send_file(s3, sqs, data_set, data_type:str, bucket_key:str):
    

    #Save data dict as csv in /tmp
    write_csv("/tmp/output.csv", data_set)
    LOGGER.info(f"Wrote local csv for: {data_set}")
    #Push to s3
    bucket_name = "group3-cafe-datatrans-bucket"
    s3.upload_file("/tmp/output.csv", bucket_name ,bucket_key)
    LOGGER.info(f"Uploading to S3 into bucket {bucket_name} with key {bucket_key}")
    #Put amessage on sqs
    message = {
        'bucket_name': bucket_name,
        'bucket_key': bucket_key,
        'data_type': data_type
    }

    json_message = json.dumps(message)

    queue_url = 'https://sqs.eu-west-1.amazonaws.com/123980920791/group3-load-queue'
    LOGGER.info(f"Sending to sqs message {json_message} to queue {queue_url}")
    sqs.send_message(
        QueueUrl= queue_url,
        MessageBody= json_message)


def write_csv(filename: str, data: 'list[dict[str,str]]'):
    with open(filename,'w') as csvfile:
        dict_writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
        dict_writer.writeheader
        dict_writer.writerows(data)


def get_filename_no_ex(filename:str):
    base_filename = os.path.splitext(filename)[0]
    return base_filename



