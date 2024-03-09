import os
import configparser
import io

config = configparser.ConfigParser()
config.read_file(io.StringIO('''[GOOGLE]
host = https://6177827686947384.4.gcp.databricks.com/
token = dapib1a4cb866568ab8dfaabc3512a5c9f70

[DEMO]
host = https://e2-demo-west.cloud.databricks.com/
token = dapif843928f124620902fee9e81c5dd39a5

[FIELD]
host = https://e2-demo-field-eng.cloud.databricks.com/
token = dapi26c802d8a3ea6bf3deea22cf4e194bbc'''))


print(config['DEMO']['host'])
print(config.sections())
exit(0)

from setuptools import find_packages, setup

setup(
    name='industry-solutions-release',
    version='1.0',
    author='Antoine Amend',
    author_email='antoine.amend@databricks.com',
    description='Deploy solution accelerators as HTML files',
    include_package_data=True,
    install_requires=[
        'databricks-api==0.9.0',
    ],
    long_description_content_type='text/markdown',
    url='https://github.com/databricks-industry-solutions/industry-solutions-release',
    packages=find_packages(where='.'),
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
        'License :: Other/Proprietary License',
    ],
)
