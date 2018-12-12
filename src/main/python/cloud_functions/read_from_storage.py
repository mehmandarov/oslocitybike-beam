from __future__ import absolute_import

import datetime
import requests

from flask import current_app
from google.cloud import storage
import six
from werkzeug import secure_filename
from werkzeug.exceptions import BadRequest


config = {
    'PROJECT_ID'               : '<MY-PROJECT-ID>',
    'CLOUD_STORAGE_BUCKET'     : 'my_oslo_bike_data',
    'ALLOWED_EXTENSIONS'       : ['txt', 'json'],
    'STATIONS_FILE_NAME'       : 'stations.txt',
    'AVAILABILITY_FILE_NAME'   : 'availability.txt',
}

def _get_storage_client():
    return storage.Client(
        project=config['PROJECT_ID'])


def _check_extension(filename, allowed_extensions):
    if ('.' not in filename or
            filename.split('.').pop().lower() not in allowed_extensions):
        raise BadRequest(
            "{0} has an invalid name or extension".format(filename))


def _safe_filename(filename):
    """
    Generates a safe filename that is unlikely to collide with existing objects
    in Google Cloud Storage.
    ``filename.ext`` is transformed into ``YYYY-MM-DD-HHMMSS-filename.ext``
    """
    filename = secure_filename(filename)
    date = datetime.datetime.utcnow().strftime("%Y-%m-%d-%H%M%S")
    basename, extension = filename.rsplit('.', 1)
    return "{0}-{1}.{2}".format(date, basename, extension)


# [START upload_file]
def upload_file(file_stream, filename, content_type):
    """
    Uploads a file to a given Cloud Storage bucket and returns the public url
    to the new object.
    """
    _check_extension(filename, config['ALLOWED_EXTENSIONS'])
    filename = _safe_filename(filename)

    client = _get_storage_client()
    bucket = client.bucket(config['CLOUD_STORAGE_BUCKET'])
    blob = bucket.blob(filename)

    blob.upload_from_string(
        file_stream,
        content_type=content_type)

    url = blob.public_url

    if isinstance(url, six.binary_type):
        url = url.decode('utf-8')

    return url
# [END upload_file]


def download_file(url, client_id, filename):
    headers = {'Client-Identifier': client_id}
    r = requests.get(url, headers=headers)
    #print(r.text)
    upload_file(r.text, filename, "application/json")

def get_bike_data(request):
    pass