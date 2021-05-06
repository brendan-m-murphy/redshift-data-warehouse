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
    new_cluster = cluster.Cluster()
    new_cluster.create()
    new_cluster.wait()
    print('* Recording cluster endpoint')
    new_cluster.write_host_to_cfg()
    print('* Opening TCP connection')
    new_cluster.open_tpc()

    print('* Infrastructure set-up complete.')
    new_cluster.summary()



if __name__ == '__main__':
    main()
