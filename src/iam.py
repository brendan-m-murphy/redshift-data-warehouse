"""
Class for role creation and management.
"""
import boto3
import botocore
import configparser
import json
import time
from src import utils


class RedshiftRole():
    """Creates and manages IAM role for Redshift cluster

    Typical usage:
    - create instance: `role = RedshiftRole()`
    - create role: `role.create()`
    - wait for the role: `role.wait_for_role()`
    - record the role arn: `role.write_arn_to_cfg()`
    - add s3 read policy: `role.allow_read_access()`
    - wait for policy: `role.wait_for_policy()`


    """
    def __init__(self):
        name, arn = utils.get_role_name_arn()
        self.name = name
        self.arn = arn
        self.client = utils.get_client('iam')
        self.resource = utils.get_resource('iam')
        self.read_policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        self.cfg_path = utils.CFG_PATH


    def create(self):
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
        try:
            response = self.client.create_role(
                RoleName = self.name,
                AssumeRolePolicyDocument = json.dumps(policy),
                Description = "Allow s3 read-only access for Redshift"
            )
        except self.client.exceptions.EntityAlreadyExistsException:
            print(f"IAM role {self.name} already exists.")
        else:
            return response


    def wait_for_role(self):
        waiter = self.client.get_waiter('role_exists')
        waiter.wait(RoleName=self.name)


    def allow_read_access(self):
        """Adds s3 read access and redshift admin access to role.

        :param role_name: string, name of role to modify
        :returns: response to iam put_role_policy method

        """
        response = self.client.attach_role_policy(
            RoleName = self.name,
            PolicyArn = self.read_policy_arn
        )
        return response


    def wait_for_policy(self):
        waiter = self.client.get_waiter('policy_exists')
        waiter.wait(PolicyArn=self.read_policy_arn)


    def write_arn_to_cfg(self):
        """Writes the arn of the role self.name to dwh.cfg
        """
        response = self.client.get_role(RoleName=self.name)
        arn = response['Role']['Arn']

        config = configparser.ConfigParser()
        config.read(self.cfg_path)
        config.set('IAM_ROLE', 'ARN', arn)
        with open(self.cfg_path, 'w') as f:
            config.write(f)


    def delete(self):
        """Delete the redshift role.

        Only delete a role *after* the Redshift cluster
        using the role has been deleted.

        """
        policy = self.resource.Policy(self.read_policy_arn)
        try:
            policy.detach_role(RoleName=self.name)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchEntity':
                print('* Policy detached.')
            else:
                raise e
        else:
            print('* Waiting for s3 read access policy to detach...')
            for n in range(31):
                if self.name in [x.name for x in policy.attached_roles.all()]:
                    if n < 30:
                        time.sleep(30)
                    else:
                        raise Exception('Policy took too long to detach.')
                else:
                    break

        print('* Deleting role...')
        self.client.delete_role(RoleName=self.name)
