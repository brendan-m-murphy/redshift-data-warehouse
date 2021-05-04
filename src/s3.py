"""
Tools for exploring dataset on s3
"""
import json
import re
from . import utils

def get_song_data(prefix=''):
    """Return all song data .json files by prefix.

    :param prefix: string of the form 'A', 'A/B', 'A/B/C', etc.

    :raises: ValueError if prefix does not have the correct form.

    :returns: list of s3 objects with specified prefixes

    """
    if not re.match(r'(([A-Z]/){0,2}[A-Z]|)$', prefix):
        raise ValueError('Invalid prefix.')
    else:
        return (utils.get_resource('s3')
                .Bucket('udacity-dend')
                .objects.filter(Prefix='song-data/' + prefix))


def get_song_samples(prefix='A/A/A', n=5):
    samples = []
    client = utils.get_client('s3')
    for i, song in enumerate(get_song_data(prefix=prefix)):
        if i > n:
            break
        else:
            obj = client.get_object(Bucket=song.bucket_name, Key=song.key)
            samples.append(json.load(obj['Body']))
    return samples


def get_log_data(year=2018, month=11, num=5):
    """Get object summaries for log data from specified period.

    It seems that we only have log data for November 2018, so
    the defaults should suffice for most data exploration.

    :param year: int or string of the form [0-9]{4}
    :param month: int (or string) between 1 and 12
    :param num: int, number of results returned
    :returns: iterator (s3.Bucket.objectsCollection) of ObjectSummary objects
    for log files

    """
    return (utils.get_resource('s3')
            .Bucket('udacity-dend')
            .objects
            .filter(Prefix='log-data/' + f'{year}/{month}')
            .limit(num))


def parse_log_object(obj):
    """Returns list of JSON objects stored in a s3 object.

    The built-it JSON parser doesn't like files containing JSON objects separated
    by newlines, which is how our log files are stored.

    :param obj: s3 ObjectSummary for an object containing JSON separated by newlines.
    :returns: list of JSON objects decoded as dictionaries

    """
    result = []
    client = utils.get_client('s3')
    response = client.get_object(Bucket=obj.bucket_name, Key=obj.key)
    body = response['Body']
    for line in body.iter_lines():
        result.append(json.loads(line))
    return result


def view_jsonpath_file():
    jsonpath = (utils.get_client('s3')
                .get_object(Bucket='udacity-dend',
                            Key='log_json_path.json'))
    return json.loads(jsonpath['Body'].read())
