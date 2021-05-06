#!/usr/bin/env python3
"""
Pause Redshift cluster
"""
from src import cluster

cluster_ = cluster.Cluster()

def pause():
    cluster_.pause()


def resume():
    cluster_.resume()


def status():
    print('Cluster status: ', cluster_.status())


def delete():
    pass
