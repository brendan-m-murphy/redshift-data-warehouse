# Introduction

# Usage

1. Create an AWS user with admin privileges and download the credentials as a .csv
2. Run `pip install -e git+https://github.com/brendan-m-murphy/udacity-dend-project-3.git` 
3. Run `config` to create `dwh.cfg`
4. Run `iac` to create a Redshift role and cluster.
5. Run `create-tables` to create tables for staging and the star schema.
6. Run `etl -y` to load all of the JSON data, or `etl -t` to load a test set.
7. Run `analytics` to try test queries.
8. Run `cleanup` to delete all AWS resources. (Or, use `pause` and `resume` to pause and resume the cluster.)
