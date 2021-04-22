# aws_utils.py
#!/home/brendan/miniconda3/envs/ml/bin/python3
"""Functions for creating and interacting with aws resources.

This is a collection of functions that will be used to create
a redshift cluster, give it permissions, and execute queries to
create tables on its database and copy files from s3.


Available functions
===================

Helper functions:
- get_auth: get auth and region info for admin
- get_cluster_config: get parameters for redshift cluster
- get_s3: get paths for buckets and JSONPath files
- get_session: create a session using dwh.cfg auth
- get_client: get a client for a given aws service
- get_resource: get a resource for a given aws service

Viewing s3 data:
-

IAM role creation:
- create_role:
- attach_policies:

Redshift cluster management:
- create_cluster: create a redshift cluster using dwh.cfg
- pause_cluster: pause the cluster
- resume_cluster: resume the cluster
- cluster_properties:
- print_cluster_info: print basic info, including cluster status

PostgreSQL functions:
- get_connection: returns a connection to the DB on our cluster
- create_table: create a table, with handling for duplicate tables
- execute: execute a query
- recent_errors: print 10 most recent errors

"""
import boto3
import configparser
import io
import json
import psycopg2
import re


# HELPER FUNCTIONS
def get_auth():
    """Extract auth and region info from 'dwh.cfg'

    :returns: Dict containing auth and region info

    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    return dict(KEY=config.get('AWS', 'KEY'),
                SECRET=config.get('AWS', 'SECRET'),
                REGION=config.get('AWS', 'REGION'))


def get_cluster_config():
    """Extract cluster config info from 'dwh.cfg'

    :returns: Dict containing cluster config

    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    return dict(CLUSTER_TYPE=config.get("CLUSTER","CLUSTER_TYPE"),
                NUM_NODES=config.get("CLUSTER","NUM_NODES"),
                NODE_TYPE=config.get("CLUSTER","NODE_TYPE"),
                CLUSTER_IDENTIFIER=config.get("CLUSTER","CLUSTER_IDENTIFIER"),
                DB_NAME=config.get("CLUSTER","DB_NAME"),
                DB_USER=config.get("CLUSTER","DB_USER"),
                DB_PASSWORD=config.get("CLUSTER","DB_PASSWORD"),
                PORT=config.get("CLUSTER","PORT"),
                IAM_ROLE_NAME=config.get("IAM_ROLE", "NAME"))


def get_s3():
    """Extract s3 bucket paths from 'dwh.cfg'

    :returns: Dict containing paths for s3 buckets and JSONPath files

    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    return dict(LOG_DATA = config.get("S3", "LOG_DATA"),
                LOG_JSONPATH = config.get("S3", "LOG_JSONPATH"),
                SONG_DATA = config.get("S3", "SONG_DATA"))


def get_session():
    """Returns a session with auth and region as in dwh.cfg

    :returns: boto3 Session in us-west-2

    """
    auth = get_auth()
    return boto3.session.Session(aws_access_key_id=auth['KEY'],
                                 aws_secret_access_key=auth['SECRET'],
                                 region_name=auth['REGION'])


def get_client(service):
    """Return a client for service, based in eu-west-2

    :param service: string for aws service, e.g. 's3', 'ec2', 'redshift', 'iam'
    :returns: boto3.client for service

    """
    session = get_session()
    return session.client(service)


def get_resource(service):
    """Return a resource for service, based in eu-west-2

    :param service: string for aws service, e.g. 's3', 'ec2'
    :returns: boto3.resource for service

    """
    session = get_session()
    return session.resource(service)


# VIEWING S3 DATA
def get_song_data(prefix=''):
    """Return all song data .json files by prefix.

    :param prefix: string of the form 'A', 'A/B', 'A/B/C', etc.

    :raises: ValueError if prefix does not have the correct form.

    :returns: list of s3 objects with specified prefixes

    """
    if not re.match(r'(([A-Z]/){0,2}[A-Z]|)$', prefix):
        raise ValueError('Invalid prefix.')
    else:
        return (get_resource('s3')
                .Bucket('udacity-dend')
                .objects.filter(Prefix='song-data/' + prefix))


def get_song_samples(n=5):
    samples = []
    client = get_client('s3')
    for i, song in enumerate(get_song_data('A/A/A')):
        if i > n:
            break
        else:
            obj = client.get_object(Bucket=song.bucket_name, Key=song.key)
            samples.append(json.load(obj['Body']))
    return samples


def get_log_data(year=2018, month=11, num=5):
    """Get object summaries for log data from specified period.

    It seems that we only have log data for November 2018, so
    the defaults should suffice for most data exploration.

    :param year: int or string of the form [0-9]{4}
    :param month: int (or string) between 1 and 12
    :param num: int, number of results returned
    :returns: iterator (s3.Bucket.objectsCollection) of ObjectSummary objects
    for log files

    """
    return (get_resource('s3')
            .Bucket('udacity-dend')
            .objects
            .filter(Prefix='log-data/' + f'{year}/{month}')
            .limit(num))


def parse_log_object(obj):
    """Returns list of JSON objects stored in a s3 object.#!/usr/bin/env python

    The built-it JSON parser doesn't like files containing JSON objects separated
    by newlines, which is how our log files are stored.

    :param obj: s3 ObjectSummary for an object containing JSON separated by newlines.
    :returns: list of JSON objects decoded as dictionaries

    """
    result = []
    client = get_client('s3')
    response = client.get_object(Bucket=obj.bucket_name, Key=obj.key)
    body = response['Body']
    for line in body.iter_lines():
        result.append(json.loads(line))
    return result


def view_jsonpath_file():
    jsonpath = (get_client('s3')
                .get_object(Bucket='udacity-dend',
                            Key='log_json_path.json'))
    return json.loads(jsonpath['Body'].read())
