"""
Class for role creation and management.
"""
import configparser
import json
from . import utils


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
        self.read_policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"


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
        iam = utils.get_client('iam')
        try:
            response = self.client.create_role(
                RoleName = self.name,
                AssumeRolePolicyDocument = json.dumps(policy),
                Description = "Allow s3 read-only access for Redshift"
            )
        except iam.exceptions.EntityAlreadyExistsException:
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
        config.read(utils.CFG_PATH)
        config.set('IAM_ROLE', 'ARN', arn)
        with open(utils.CFG_PATH, 'w') as f:
            config.write(f)


    def delete(self):
        """Delete an IAM role.

        TODO implement

        Needs to remove policies before deleting.
        Need policy ARNs to do so.

        :returns:

        """
        pass
