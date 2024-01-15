from boto3.session import *
import os
access_key = ''
secret_key = ''
bucket = 'stack-overflow-analysis-bucket'
s3_output_prefix = 'raw_data_dump'
session = Session(
  aws_access_key_id=access_key, 
  aws_secret_access_key=secret_key
)
s3s = session.resource('s3').Bucket(bucket)
local_input_prefix = '/home/ubuntu'
data_file = 'Posts.xml'
input_path = os.path.join(local_input_prefix, data_file)
output_path = os.path.join(s3_output_prefix, data_file)
s3s.upload_file(input_path, output_path)