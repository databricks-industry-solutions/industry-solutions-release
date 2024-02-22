import shutil
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
parser.add_argument("-d", "--deploy", help="deploy solution to S3", action='store_false')
parser.add_argument("-v", "--vertical", help="industry vertical [fsi, rcpg, cme, hls, manuf]")

args = parser.parse_args()

if not args.path:
    print("please provide a databricks path to download solution from")
    sys.exit(1)

if not args.name:
    print("please provide a code name for this solution accelerator")
    sys.exit(1)

if args.deploy:
    if not args.vertical:
        print("please provide an industry vertical")
        sys.exit(1)
    else:
        industries = ['fsi', 'rcg', 'cme', 'hls', 'mnf']
        if args.vertical not in industries:
            print("please provide a valid industry vertical, {}".format(industries))
            sys.exit(1)

output_dir = f'dist/{args.name}'
if os.path.exists(output_dir):
    shutil.rmtree(output_dir)
os.makedirs(output_dir)

accelerator = Accelerator(
    db_host='e2-demo-west.cloud.databricks.com',
    db_token=os.environ['DB_TOKEN'],
    deploy=False,
    aws_s3_bucket='databricks-web-files',
    aws_s3_path='notebooks/{vertical}/{solution_codename}/{file_name}',
    aws_s3_link='https://databricks-web-files.s3.us-east-2.amazonaws.com/notebooks',
    aws_access_key_id=os.environ['AWS_ACCESS_KEY'],
    aws_secret_access_key=os.environ['AWS_ACCESS_SECRET']
)

accelerator.release(
    db_path=args.path,
    db_name=args.name,
    industry_vertical=args.vertical.lower(),
    output_dir=output_dir
)