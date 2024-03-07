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

args = parser.parse_args()

accelerator = Accelerator(
    db_host=os.environ['DB_HOST'],
    db_token=os.environ['DB_TOKEN']
)

accelerator.release(
    db_path=args.path,
    db_name=args.name
)
