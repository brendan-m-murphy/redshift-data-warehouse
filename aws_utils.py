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
import psycopg2


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
