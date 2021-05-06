"""
Provides a class for creating and interacting with a Redshift cluster
"""
import configparser
from src import utils


class Cluster():
    """Creates and manages a Redshift cluster
    """
    def __init__(self):
        """Creates a Cluster instance.

        A Cluster object can:
        - create the dwh cluster
        - return or print the cluster's properties
        - pause or resume the cluster

        :returns:

        """
        self.client = utils.get_client('redshift')
        self.config = utils.get_cluster_config()
        self.db_config = utils.get_db_config()
        self.cfg_path = utils.CFG_PATH

        _, arn = utils.get_role_name_arn()
        if not arn:
            self.role_arn = None
        else:
            self.role_arn = arn


    def set_role_arn(self, arn):
        """Store the arn of IAM role

        This role should be written to dwh.cfg by RedshiftRole.

        :param arn: Amazon Resource Number for an IAM role

        """
        if arn:
            self.role_arn = arn
        else:
            raise ValueError("arn cannot be empty string")


    def create(self):
        """Create a redshift cluster based on info in dwh.cfg

        :returns: response to redshift client create_cluster method

        """
        if not self.role_arn:
            _, arn = utils.get_role_name_arn()
            self.set_role_arn(arn)  # raises ValueError if arn is not set

        try:
            response = self.client.create_cluster(
                DBName = self.db_config['NAME'],
                ClusterIdentifier = self.config['CLUSTER_IDENTIFIER'],
                ClusterType = self.config['CLUSTER_TYPE'],
                NodeType = self.config['NODE_TYPE'],
                MasterUsername = self.db_config['USER'],
                MasterUserPassword = self.db_config['PASSWORD'],
                Port = int(self.db_config['PORT']),
                NumberOfNodes = int(self.config['NUM_NODES']),
                IamRoles = [self.role_arn]
            )
        except self.client.exceptions.ClusterAlreadyExistsFault:
            print(f"Cluster {self.config['CLUSTER_IDENTIFIER']} already exists.")
        else:
            return response


    def properties(self):
        """Return description of redshift cluster
        """
        ci = self.config['CLUSTER_IDENTIFIER']
        return self.client.describe_clusters(ClusterIdentifier=ci)['Clusters'][0]


    def summary(self):
        """Prints basic info about redshift cluster
        """
        keys = ['ClusterIdentifier', 'NodeType', 'ClusterStatus',
                    'MasterUsername', 'DBName', 'Endpoint',
                    'NumberOfNodes', 'VpcId']
        properties = self.properties()
        print('\nCluster properties:\n')
        for k in keys:
            print(f'{k:<20}{properties[k]}')


    def status(self):
        """Return cluster status
        """
        return self.properties()['ClusterStatus']


    def pause(self):
        """Pause redshift cluster

        The cluster cannot be paused without an initial snapshot,
        so this method creates a snapshot, if none exist, before
        pausing.

        """
        ci = self.config["CLUSTER_IDENTIFIER"]
        try:
            response = self.client.pause_cluster(ClusterIdentifier=ci)
        except client.exceptions.InvalidClusterStateFound:
            print('Cluster cannot be paused because no recent backup found.\nCreating a snapshot...')
            self. client.create_cluster_snapshot(SnapshotIdentifier="pause-cluster-snap",
                                                 ClusterIdentifier=ci)
            print("Waiting for snapshot. This could take up to 5 minutes.")
            waiter = self.client.get_waiter('snapshot_available')
            waiter.wait(SnapshotIdentifier="pause-cluster-snap",
                        ClusterIdentifier=ci)
            print("Snapshot ready. Pausing...")
            response = self.client.pause_cluster(ClusterIdentifier=ci)
        return response


    def resume(self):
        "Resume paused redshift cluster"
        ci = self.config["CLUSTER_IDENTIFIER"]
        response = self.client.resume_cluster(ClusterIdentifier=ci)
        return response


    def wait(self):
        """Wait for the Redshift cluster to be available.

        Use after creating the cluster or resuming the cluster to
        ensure that the cluster is available before trying to access it.

        """
        cluster_waiter = self.client.get_waiter('cluster_available')
        ci = self.config['CLUSTER_IDENTIFIER']
        print("* Waiting for cluster")
        cluster_waiter.wait(ClusterIdentifier=ci)
        print("* Cluster available")


    def write_host_to_cfg(self):
        """Write the cluster endpoint address to HOST in dwh.cfg

        Run this after the cluster is first available to record the DB
        host address, which is needed for creating a connection to the DB.

        """
        host = self.properties()['Endpoint']['Address']

        config = configparser.ConfigParser()
        config.read(self.cfg_path)
        config.set('DB', 'HOST', host)
        with open(self.cfg_path, 'w') as f:
                config.write(f)


    def open_tpc(self):
        """Open an incoming TCP port to access the cluster endpoint

        The cluster must be available before this is run.

        """
        ec2 = utils.get_resource('ec2')
        vpc_id = self.properties()['VpcId']
        vpc = ec2.Vpc(id=vpc_id)
        default_sg = list(vpc.security_groups.all())[0]
        PORT = self.db_config['PORT']

        default_sg.authorize_ingress(
            GroupName = default_sg.group_name,
            CidrIp = "0.0.0.0/0",
            IpProtocol = "TCP",
            FromPort = int(PORT),
            ToPort = int(PORT)
        )


    def delete(self):
        ci = self.config['CLUSTER_IDENTIFIER']
        try:
            self.client.delete_cluster(ClusterIdentifier=ci,
                                       SkipFinalClusterSnapshot=True)
        except self.client.exceptions.ClusterNotFoundFault:
            print('* Cluster not found. It may have already been deleted.')
        else:
            print('* Waiting until cluster deleted...')
            waiter = self.client.get_waiter('cluster_deleted')
            waiter.wait(ClusterIdentifier=ci)
            print('* Cluster deleted')
