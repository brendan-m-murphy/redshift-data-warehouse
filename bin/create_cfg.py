#!/usr/bin/env python3
"""Creates .cfg file for data warehouse project

Optional argument: output file name. Default is 'dwh.cfg'.

How to use this script:
- download admin credentials as a csv file (default: 'new_user_credentials.csv')
- run the script
- if you don't specify an output filename, dwh.cfg will be used, possibly
  over-writing the existing config, which would delete the role arn and
  host address
- answer the prompts to modify the config, or hit return to accept defaults

Warning: this script does not perform input validation, so modify the
defaults at your own risk.

"""
import argparse
import configparser
import csv
import os


def read_cred_csv(cred_file):
    """
    Read credentials from .csv file downloaded from AWS.
    """
    # open with encoding utf-8-sig, incase of BOM
    with open(cred_file, 'r', newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=',')
        row = next(reader)
        key = row["Access key ID"]
        secret = row["Secret access key"]
    return key, secret


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input',
                        help="Specify input .csv file with AWS credentials (column 2 = KEY, column 3 = SECRET)")
    args = parser.parse_args()

    OUT_FILE = 'src/cfg/dwh.cfg'
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)

    if args.input:
        CRED_FILE = args.input
    else:
        CRED_FILE = 'new_user_credentials.csv'

    use_default = input("Use default parameters? [y/n] ")
    while use_default not in ['y', 'n']:
        use_default = input("Enter 'y' or 'n': ")

    # [AWS]
    aws = {'REGION': 'us-west-2'}

    # read credentials from .csv file
    aws['KEY'], aws['SECRET'] = read_cred_csv(CRED_FILE)

    # [CLUSTER]
    cluster = {'CLUSTER_TYPE': 'multi-node',
               'NUM_NODES': '4',
               'NODE_TYPE': 'dc2.large',
               'CLUSTER_IDENTIFIER': 'sparkify-cluster'}

    # [DB]
    db = {'HOST': '',
          "NAME": 'dwh',
          "USER": 'sparkifier',
          "PASSWORD": 'Passw0rd',
          "PORT": '5439'}

    # [IAM_ROLE]
    iam = {'NAME': 'sparkify_redshift_role',
           'ARN': ''}

    # [S3]
    s3 = {'LOG_DATA': 's3://udacity-dend/log-data',
          'LOG_JSONPATH': 's3://udacity-dend/log_json_path.json',
          'SONG_DATA': 's3://udacity-dend/song-data'}

    if use_default == 'n':
        print("Enter new value, or hit return to accept default.")

        if x := input("Enter a AWS region (default 'us-west-2'): "):
            aws['REGION'] = x

        if x := input("Enter a cluster identifier (default 'sparkify-cluster'): "):
            cluster['CLUSTER_IDENTIFIER'] = x

        if x := input("Enter a database name (default 'dwh'): "):
            db['NAME'] = x

        if x := input("Enter a database username (default 'sparkifier'): "):
            db['USER'] = x

        if x := input("Enter a database password (default 'Passw0rd'): "):
            db['DB_PASSWORD'] = x

        if x := input("Enter an iam['IAM'] role name (default 'sparify_redshift_role'): "):
            iam['NAME'] = x

    # Create config file
    config = configparser.ConfigParser()
    config['AWS'] = aws
    config['CLUSTER'] = cluster
    config['DB'] = db
    config['IAM_ROLE'] = iam
    config['S3'] = s3

    with open(OUT_FILE, 'w') as f:
        config.write(f)


if __name__ == '__main__':
    main()
