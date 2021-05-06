#!/usr/bin/env python3
import boto3
import configparser


CFG_PATH = 'src/cfg/dwh.cfg'

# configs
def get_auth():
    """Extract auth and region info from CFG_PATH

    :returns: Dict containing auth and region info

    """
    config = configparser.ConfigParser()
    config.read_file(open(CFG_PATH))

    return dict(KEY=config.get('AWS', 'KEY'),
                SECRET=config.get('AWS', 'SECRET'),
                REGION=config.get('AWS', 'REGION'))


def get_cluster_config():
    """Extract cluster config info from CFG_PATH

    :returns: Dict containing cluster config

    """
    config = configparser.ConfigParser()
    config.read_file(open(CFG_PATH))

    return dict(CLUSTER_TYPE=config.get("CLUSTER","CLUSTER_TYPE"),
                NUM_NODES=config.get("CLUSTER","NUM_NODES"),
                NODE_TYPE=config.get("CLUSTER","NODE_TYPE"),
                CLUSTER_IDENTIFIER=config.get("CLUSTER","CLUSTER_IDENTIFIER"))


def get_db_config():
    config = configparser.ConfigParser()
    config.read_file(open(CFG_PATH))
    return dict(NAME=config.get("DB","NAME"),
                USER=config.get("DB","USER"),
                PASSWORD=config.get("DB","PASSWORD"),
                PORT=config.get("DB","PORT"),
                HOST=config.get("DB", "HOST"))


def get_s3_config():
    """Extract s3 bucket paths from CFG_PATH

    :returns: Dict containing paths for s3 buckets and JSONPath files

    """
    config = configparser.ConfigParser()
    config.read_file(open(CFG_PATH))

    return dict(LOG_DATA = config.get("S3", "LOG_DATA"),
                LOG_JSONPATH = config.get("S3", "LOG_JSONPATH"),
                SONG_DATA = config.get("S3", "SONG_DATA"))


def get_role_name_arn():
    """Return redshift role name and arn.

    :returns: 2-tuple of strings: iam role name, iam role arn

    """
    config = configparser.ConfigParser()
    config.read_file(open(CFG_PATH))

    return config.get("IAM_ROLE", "NAME"), config.get("IAM_ROLE", "ARN")


# boto3 helpers
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
