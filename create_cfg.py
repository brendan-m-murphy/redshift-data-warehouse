#!/usr/bin/env python3
"""Creates .cfg file for data warehouse project

Optional argument: output file name. Default is 'dwh.cfg'.

How to use this script:
- download admin credentials as a csv file 'new_user_credentials.csv'
- run the script
- if you don't specify an output filename, dwh.cfg will be used, possibly
  over-writing the existing config, which would delete the role arn and
  host address
- answer the prompts to modify the config, or hit return to accept defaults

Warning: this script does not perform input validation, so modify the
defaults at your own risk.

"""
import configparser
import csv
import sys


def main(*args):
    if len(args) > 1:
        outfile = args[1]
    else:
        outfile = 'dwh.cfg'

    config = configparser.ConfigParser()

    # [AWS]
    config['AWS'] = {}
    aws = config['AWS']

    CRED_FILE = 'new_user_credentials.csv'
    with open(CRED_FILE, 'r', newline='') as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)
        row = next(reader)
        aws['KEY'] = row[2]
        aws['SECRET'] = row[3]

    print("* Enter parameters, or hit return to accept default.")

    x = input("Enter a AWS region (default 'us-west-2'): ")
    aws['REGION'] = x or 'us-west-2'


    # [CLUSTER]
    config['CLUSTER'] = {'CLUSTER_TYPE': 'multi-node',
                         'NUM_NODES': '4',
                         'NODE_TYPE': 'dc2.large'}
    cluster = config['CLUSTER']

    x = input("Enter a cluster identifier (default 'sparkify-cluster'): ")
    cluster['CLUSTER_IDENTIFIER'] = x or 'sparkify-cluster'

    cluster['HOST'] = ''

    x = input("Enter a database name (default 'dwh'): ")
    cluster['DB_NAME'] = x or 'dwh'

    x = input("Enter a database username (default 'sparkifier'): ")
    cluster['DB_USER'] = x or 'sparkifier'

    x = input("Enter a database password (default 'Passw0rd'): ")
    cluster['DB_PASSWORD'] = x or 'Passw0rd'

    cluster['DB_PORT'] = '5439'

    # [IAM_ROLE]
    config['IAM_ROLE'] = {}
    iam = config['IAM_ROLE']
    x = input("Enter an iam['IAM'] role name (default 'sparify_redshift_role'): ")
    iam['NAME'] = x or 'sparkify_redshift_role'

    iam['ARN'] = ''

    # [S3]
    config['S3'] = {'LOG_DATA': 's3://udacity-dend/log-data',
                     'LOG_JSONPATH': 's3://udacity-dend/log_json_path.json',
                     'SONG_DATA': 's3://udacity-dend/song-data'}

    # Write config file
    with open(outfile, 'w') as f:
        config.write(f)


if __name__ == '__main__':
    main(*sys.argv)
