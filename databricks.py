from databricks.accelerator import Accelerator
import os
import logging
import sys
import argparse

root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

parser = argparse.ArgumentParser()
parser.add_argument("-p", "--path", help="databricks path")
parser.add_argument("-n", "--name", help="databricks solution code name, default to path basename")

args = parser.parse_args()


'''
    name: databricks-web-files
    path: notebooks/{vertical}/{solution_codename}/{file_name}
    link: https://databricks-web-files.s3.us-east-2.amazonaws.com/notebooks
    '''


s3_bucket = 'databricks-web-files'
s3_path = 'notebooks/{solution_codename}/{file_name}'
s3_link = 'https://databricks-web-files.s3.us-east-2.amazonaws.com/notebooks'
s3_access_key = os.environ['AWS_ACCESS_KEY']
s3_secret_key = os.environ['AWS_ACCESS_SECRET']

db_host = os.environ['DB_HOST']
db_token = os.environ['DB_TOKEN']



accelerator = Accelerator()

accelerator.release(
    db_path=args.path,
    db_name=args.name
)

s3_host = os.environ['AWS_ACCESS_KEY']
s3_key = os.environ['AWS_ACCESS_SECRET']
