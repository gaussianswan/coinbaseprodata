# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from s3helper import S3 
from dotenv import load_dotenv
import os 

load_dotenv()

api_key = os.environ['AWS_ACCESS_KEYID']
secret_key = os.environ['AWS_SECRET_ACCESSKEY']
region = 'us-east-1'

s3 = S3(api_key, secret_key, region)

# Now, we want to upload the parquet files
bucket_name = 'coinbaseminutepricingdata'
# s3.create_new_bucket(new_bucket_name = 'coinbaseminutepricingdata')

filenames = ['BTC-USD_minute_data.parquet', 'ETH-USD_minute_data.parquet', 'LTC-USD_minute_data.parquet']
for file in filenames: 
    s3.upload(bucket_name, from_filename=file, to_filename=file)
    
print("Done uploading")


