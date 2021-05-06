#!/usr/bin/env python3

from setuptools import setup, find_packages

with open('requirements.txt', 'r') as f:
    REQUIRE = [line for line in f]


setup(
    name = 'project3',
    description= 'Creating a data warehouse with Amazon Redshift.',
    author = 'Brendan Murphy',
    url = 'https://github.com/brendan-m-murphy/udacity-dend-project-3',
    packages = find_packages(include=['src', 'src.*']),
    install_requires = REQUIRE,
    entry_points = {
        'console_scripts': [
            'config=bin.create_cfg:main',
            'create-tables=bin.create_tables:main',
            'etl=bin.etl:main',
            'iac=bin.iac:main',
            'pause=bin.status:pause',
            'resume=bin.status:resume',
            'status=bin.status:status',
            'cleanup=bin.status:cleanup',
            'analytics=bin.analytics:main'
        ]
    }
)
