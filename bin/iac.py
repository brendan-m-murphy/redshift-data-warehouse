#!/usr/bin/env python3
from src import cluster, iam


def main():
    print('* Creating redshift role')
    role = iam.RedshiftRole()
    role.create()
    role.wait_for_role()
    print('* Recording role arn')
    role.write_arn_to_cfg()
    print('* Attaching s3 read access policy')
    role.allow_read_access()
    role.wait_for_policy()

    print('* Launching Redshift cluster')
    cluster = cluster.Cluster()
    cluster.create()
    cluster.wait()
    print('* Recording cluster endpoint')
    cluster.write_host_to_cfg()
    print('* Opening TCP connection')
    cluster.open_tpc()

    print('* Infrastructure set-up complete.')
    cluster.summary()



if __name__ == '__main__':
    main()
