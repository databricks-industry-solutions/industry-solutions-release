import shutil
import yaml
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
parser.add_argument("-c", "--config", help="local configuration")
parser.add_argument("-d", "--deploy", help="deploy solution to S3", action='store_true')
parser.add_argument("-v", "--vertical", help="industry vertical [fsi, rcpg, cme, hls, manuf]")

args = parser.parse_args()
if not args.config or not os.path.exists(args.config):
    print("please provide a valid configuration file")
    sys.exit(1)

print(args.deploy)

if not args.path:
    print("please provide a databricks path to download solution from")
    sys.exit(1)

if args.deploy:
    if not args.vertical:
        print("please provide an industry vertical")
        sys.exit(1)
    else:
        industries = ['fsi', 'rcpg', 'cme', 'hls', 'manuf']
        if args.vertical not in industries:
            print("please provide a valid industry vertical, {}".format(industries))
            sys.exit(1)

output_dir = 'dist'
if os.path.exists(output_dir):
    shutil.rmtree(output_dir)
os.mkdir(output_dir)

config = yaml.safe_load(open(args.config, 'r'))
accelerator = Accelerator(
    db_host=config['databricks']['host'],
    db_token=config['databricks']['token'],
    deploy=args.deploy,
    aws_s3_bucket=config['aws']['bucket']['name'],
    aws_s3_path=config['aws']['bucket']['path'],
    aws_s3_link=config['aws']['bucket']['link'],
    aws_access_key_id=config['aws']['access_key_id'],
    aws_secret_access_key=config['aws']['secret_access_key']
)

accelerator.release(
    db_path=args.path,
    industry_vertical=args.vertical.lower(),
    output_dir=output_dir
)