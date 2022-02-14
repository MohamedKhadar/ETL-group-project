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
    
    # print(transformed_data[0])
    creds = db.get_ssm_parameters_under_path('/team3/redshift')
    for order in transformed_data:
        LOGGER.info('inserting order into database')
        db.insert_order(creds, order)
    
