#!/usr/bin/env python3
"""
Check and modify the status of the Redshift cluster
"""
from src import cluster, iam

cluster_ = cluster.Cluster()

def pause():
    """Pause the Redshift cluster
    """
    cluster_.pause()


def resume():
    """Resume the Redshift cluster
    """
    cluster_.resume()


def status():
    """Print cluster status
    """
    print('Cluster status: ', cluster_.status())


def cleanup():
    """Delete Redshift cluster and redshift_role
    """
    cluster_.delete()
    iam.RedshiftRole().delete()
    print('* Clean up complete.')
