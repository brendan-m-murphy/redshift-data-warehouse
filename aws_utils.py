# aws_utils.py
#!/usr/bin/env python3
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
import botocore
import configparser
import io
import json
from mypy_boto3_cloudformation.client import BotocoreClientError
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
                PORT=config.get("CLUSTER","DB_PORT"),
                IAM_ROLE_NAME=config.get("IAM_ROLE", "NAME"))


def get_s3_config():
    """Extract s3 bucket paths from 'dwh.cfg'

    :returns: Dict containing paths for s3 buckets and JSONPath files

    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    return dict(LOG_DATA = config.get("S3", "LOG_DATA"),
                LOG_JSONPATH = config.get("S3", "LOG_JSONPATH"),
                SONG_DATA = config.get("S3", "SONG_DATA"))


def get_role_name_arn():
    """Return redshift role name and arn.

    :returns: 2-tuple of strings: iam role name, iam role arn

    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    return config.get("IAM_ROLE", "NAME"), config.get("IAM_ROLE", "ARN")


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
    """Returns list of JSON objects stored in a s3 object.

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


# IAM role creation
def create_redshift_role(role_name):
    """Create an IAM role that can be assumed by redshift.


    :param role_name: string, name of the role
    :returns: response to iam create_role method

    """
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "sts:AssumeRole",
                "Principal": {
                    "Service": "redshift.amazonaws.com"
                }
            }
        ]
    }
    iam = get_client('iam')
    try:
        response = iam.create_role(
            RoleName = role_name,
            AssumeRolePolicyDocument = json.dumps(policy),
            Description = "Allow s3 read-only access for Redshift"
        )
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"IAM role {role_name} already exists.")
    else:
        return response


def allow_read_access(role_name):
    """Adds s3 read access and redshift admin access to role.

    :param role_name: string, name of role to modify
    :returns: response to iam put_role_policy method

    """
    iam = get_client('iam')
    response = iam.attach_role_policy(
        RoleName = role_name,
        PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )
    return response


def write_role_arn_to_cfg(role_name):
    """Writes the arn of role_name to dwh.cfg

    :param role_name: name of the IAM role

    """
    iam = get_client('iam')
    response = iam.get_role(RoleName=role_name)
    arn = response['Role']['Arn']

    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    config.set('IAM_ROLE', 'ARN', arn)
    with open('dwh.cfg', 'w') as f:
        config.write(f)


def delete_role(role_name):
    """Delete an IAM role.

    TODO implement

    Needs to remove policies before deleting.
    Need policy ARNs to do so.

    :returns:

    """
    pass


# Redshift cluster management
def create_cluster():
    """Create a redshift cluster based on info in dwh.cfg

    :returns: response to redshift client create_cluster method

    """
    client = get_client('redshift')
    config = get_cluster_config()
    _, arn = get_role_name_arn()

    if not arn:
        raise Exception("create_cluster exited: IAM role ARN empty.")

    response = client.create_cluster(
        DBName = config['DB_NAME'],
        ClusterIdentifier = config['CLUSTER_IDENTIFIER'],
        ClusterType = config['CLUSTER_TYPE'],
        NodeType = config['NODE_TYPE'],
        MasterUsername = config['DB_USER'],
        MasterUserPassword = config['DB_PASSWORD'],
        Port = int(config['PORT']),
        NumberOfNodes = int(config['NUM_NODES']),
        IamRoles = [arn]
    )
    return response


def redshift_properties():
    """Return description of redshift cluster

    :returns: description of redshift cluster

    """
    client = get_client('redshift')
    ci = get_cluster_config()['CLUSTER_IDENTIFIER']
    return client.describe_clusters(ClusterIdentifier=ci)['Clusters'][0]


def print_redshift_properties():
    """Prints basic info about redshift cluster

    :returns: None

    """
    keys = ['ClusterIdentifier', 'NodeType', 'ClusterStatus',
                'MasterUsername', 'DBName', 'Endpoint',
                'NumberOfNodes', 'VpcId']
    properties = redshift_properties()
    print('\nCluster properties:\n')
    for k in keys:
        print(f'{k:<20}{properties[k]}')


def pause_cluster():
    "Pause redshift cluster"
    client = get_client('redshift')
    ci = get_cluster_config()["CLUSTER_IDENTIFIER"]
    try:
        response = client.pause_cluster(ClusterIdentifier=ci)
    except client.exceptions.InvalidClusterStateFound:
        print('Cluster cannot be paused because no recent backup found.\nCreating a snapshot...')
        client.create_cluster_snapshot(SnapshotIdentifier="pause-cluster-snap",
                                       ClusterIdentifier=ci)
        print("Waiting for snapshot. This could take up to 5 minutes.")
        waiter = client.get_waiter('snapshot_available')
        waiter.wait(SnapshotIdentifier="pause-cluster-snap",
                    ClusterIdentifier=ci)
        print("Snapshot ready. Pausing...")
        response = client.pause_cluster(ClusterIdentifier=ci)
    return response


def resume_cluster():
    "Resume paused redshift cluster"
    client = get_client('redshift')
    ci = get_cluster_config()["CLUSTER_IDENTIFIER"]
    response = client.resume_cluster(ClusterIdentifier=ci)
    return response


def write_cluster_host_to_cfg():
    """Write the cluster endpoint address to HOST in dwh.cfg
    """
    host = redshift_properties()['Endpoint']['Address']

    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    config.set('CLUSTER', 'HOST', host)
    with open('dwh.cfg', 'w') as f:
            config.write(f)



def open_tpc():
    """Open an incoming TCP port to access the cluster endpoint

    :returns: None

    """
    ec2 = get_resource('ec2')
    vpc_id = redshift_properties()['VpcId']
    vpc = ec2.Vpc(id=vpc_id)
    default_sg = list(vpc.security_groups.all())[0]
    PORT = get_cluster_config()['PORT']

    default_sg.authorize_ingress(
        GroupName = default_sg.group_name,
        CidrIp = "0.0.0.0/0",
        IpProtocol = "TCP",
        FromPort = int(PORT),
        ToPort = int(PORT)
    )


# PostgreSQL functions:
def get_connection():
    """Get connection to Postgres database on Redshift cluster

    :returns: a psycopg2 connection to DB

    """
    config = get_cluster_config()
    conn = psycopg2.connect(
        dbname = config['DB_NAME'],
        user = config['DB_USER'],
        password = config['DB_PASSWORD'],
        host = config['HOST'],
        port = config['PORT']
    )
    return conn


def create_table(query):
    """Creates a table according to the passed query.

    :param query: str, PostgreSQL create table query

    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(query)
    except psycopg2.Error as e:
        if e.pgcode == "42P07":
            print("Table already exists:", e)
        else:
            raise
    else:
        conn.commit()
    finally:
        cur.close()
        conn.close()


# def parallel_copy():
#     """Copy prefix partitioned files from s3 bucket to postgres table.

#     :returns: None

#     """
#     _, IAM_ROLE_ARN = get_role_name_arn()
#     query = f"""
#     COPY sporting_event_ticket
#     FROM 's3://udacity-labs/tickets/split/part'
#     CREDENTIALS 'aws_iam_role={IAM_ROLE_ARN}'
#     GZIP
#     DELIMITER ';'
#     COMPUPDATE OFF
#     REGION 'us-west-2';
#     """
#     with get_connection() as conn:
#         with conn.cursor() as cur:
#             cur.execute(query)


def check_load_errors():
    """Print last 10 load errors from stl_load_errors, with detailed info

    :returns: None

    """
    query = """
    SELECT le.starttime, d.query, d.line_number, d.colname, d.value, le.err_reason
    FROM stl_loaderror_detail AS d
    JOIN stl_load_errors AS le ON d.query = le.query
    ORDER BY le.starttime DESC
    LIMIT 10;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            for item in cur.fetchall():
                print(item)


# Infrastructure set-up
def main():
    # create role
    print('* Creating redshift role')
    role_name, _ = get_role_name_arn()
    create_redshift_role(role_name)


    iam_client = get_client('iam')
    create_role_waiter = iam_client.get_waiter('role_exists')
    create_role_waiter.wait(RoleName=role_name)

    # record arn
    print('* Recording role arn')
    write_role_arn_to_cfg(role_name)
    _, role_arn = get_role_name_arn()

    # attach policies
    print('* Attaching s3 read access policy')
    allow_read_access(role_name)
    # policy_waiter = iam_client.get_waiter('policy_exists')
    # policy_waiter.wait()

    # launch redshift cluster
    print('* Launching Redshift cluster')
    create_cluster()
    redshift_client = get_client('redshift')
    cluster_waiter = redshift_client.get_waiter('cluster_available')
    ci = get_cluster_config()['CLUSTER_IDENTIFIER']
    cluster_waiter.wait(ClusterIdentifier=ci)

    # record endpoint
    print('* Recording cluster endpoint')
    write_cluster_host_to_cfg()

    # open TCP connection
    print('* Opening TCP connection')
    open_tpc()

    print('* Infrastructure complete!')


if __name__ == '__main__':
    main()
