# Introduction

A (fictional) music streaming start-up, *Sparkify*, wants a data warehouse to analyze what users are listening to.

Their music library and log data about user events are stored as JSON files on AWS s3.
The goal of this project is make the data easy to analyze.
To accomplish this, we load the data into a Redshift cluster with a star schema database.

# Schema

The center of the star schema ("fact table") is the `songplays` table, which contains the following columns:
- `songplay_id`: primary key
- `start_time`: when the song was played, also foreign key to `time` table
- `user_id`: foreign key to `users` table
- `level`: free or paid tier
- `song_id`: foreign key to `songs` table
- `artist_id`: foreign key to `artists` table
- `session_id`: id for a continuous user session
- `location`: where the song was played
- `user_agent`: how the song was played (e.g. which browser was used)

The points of the star schema ("dimension tables") are:
- `songs`: `song_id`, (song) `title`, `artist_id`, `year` (released), (song) `duration`
- `artists`: `artist_id`, (artist) `name`, (artist) `location`, `lattitude`, and `longitude`
- `users`: `user_id`, `first_name`, `last_name`, `gender`, `level` (free or paid tier)
- `time`: `start_time`, `hour`, `day`, `week`, `month`, `year`, `weekday` (true/false)

# Organization

- The `src` module contains code for creating and managing a Redshift cluster, along with SQL queries used in the scripts.
- The `bin` directory contains scripts for creating AWS infrastructure, creating SQL tables, loading the tables, and querying the database.
- Installing the repo locally allows the user to run everything from the command line. See below.

# Usage

1. Create an AWS user with admin privileges and download the credentials as a .csv
2. Run `pip install -e git+https://github.com/brendan-m-murphy/udacity-dend-project-3.git#egg=project3` to install a local copy of the project. 
3. Run `config` to create `dwh.cfg`
4. Run `iac` to create a Redshift role and cluster.
5. Run `create-tables` to create tables for staging and the star schema.
6. Run `etl -y` to load all of the JSON data, or `etl -t` to load a test set.
7. Run `analytics` to try test queries.
8. Run `cleanup` to delete all AWS resources. (Or, use `pause` and `resume` to pause and resume the cluster.)

Note: these scripts should be run in the directory where the repo is installed.
