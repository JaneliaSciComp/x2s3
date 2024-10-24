import time
import multiprocessing

import boto3
import pytest
from pydantic import HttpUrl

from x2s3.app import create_app
from x2s3.settings import Target, Settings

# Set the start method to spawn to avoid pickling issues
multiprocessing.set_start_method('spawn', force=True)

PORT = 12392

def get_settings():
    settings = Settings()
    settings.targets = [
        Target(
            name='janelia-data-examples',
            options={'bucket':'janelia-data-examples'}
        )
    ]
    return settings


def run_server():
    import uvicorn
    app = create_app(get_settings)
    uvicorn.run(app, host="0.0.0.0", port=PORT)

@pytest.fixture(scope="module")
def app():
    process = multiprocessing.Process(target=run_server)
    process.start()
    time.sleep(2)  # Give the server a moment to start
    yield app
    process.terminate()
    process.join()


@pytest.fixture
def s3_client():
    # Avoid "botocore.exceptions.NoCredentialsError: Unable to locate credentials" by giving dummy credentials
    return boto3.client('s3', endpoint_url=f"http://localhost:{PORT}", region_name='us-east-1', aws_access_key_id='NONE', aws_secret_access_key='NONE')


def test_acl(app, s3_client):
    response = s3_client.get_bucket_acl(Bucket='janelia-data-examples')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(response['Grants']) == 1


def test_list_objects(app, s3_client):
    response = s3_client.list_objects_v2(Bucket='janelia-data-examples', MaxKeys=7)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert response['Name'] == 'janelia-data-examples'
    assert 'CommonPrefixes' not in response
    assert len(response['Contents']) <= 7
    if 'NextContinuationToken' in response:
        assert response['IsTruncated'] is True
    assert 'Key' in response['Contents'][0]
    assert 'Size' in response['Contents'][0]
    assert 'LastModified' in response['Contents'][0]


def test_list_objects_delimiter(app, s3_client):
    response = s3_client.list_objects_v2(Bucket='janelia-data-examples', Delimiter='/', MaxKeys=9)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert response['Name'] == 'janelia-data-examples'
    assert response['Delimiter'] == '/'
    assert len(response['CommonPrefixes']) >= 1
    assert response['IsTruncated'] is False


def test_list_objects_continuation(app, s3_client):
    max_keys = 4
    prefix = 'jrc_mus_lung_covid.n5/render/v1_acquire_align___20210609_224836/s0/0/0'
    total = 0
    continuation_token = None
    while True:
        if continuation_token:
            response = s3_client.list_objects_v2(Bucket='janelia-data-examples', Prefix=prefix, MaxKeys=max_keys, ContinuationToken=continuation_token)
        else:
            response = s3_client.list_objects_v2(Bucket='janelia-data-examples', Prefix=prefix, MaxKeys=max_keys)
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
        assert response['Name'] == 'janelia-data-examples'
        assert response['MaxKeys'] == max_keys
        assert 'CommonPrefixes' not in response
        total += len(response['Contents'])
        assert len(response['Contents']) <= max_keys
        if 'NextContinuationToken' in response:
            assert response['IsTruncated'] is True
            continuation_token = response['NextContinuationToken']
        else:
            break
    assert total == 6


def test_head_object(app, s3_client):
    response = s3_client.head_object(Bucket='janelia-data-examples', Key='jrc_mus_lung_covid.n5/attributes.json')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    with pytest.raises(s3_client.exceptions.ClientError) as exc_info:
        s3_client.head_object(Bucket='janelia-data-examples', Key='jrc_mus_lung_covid.n5/')
    assert exc_info.value.response['Error']['Code'] == '404'


def test_get_object(app, s3_client):
    response = s3_client.get_object(Bucket='janelia-data-examples', Key='jrc_mus_lung_covid.n5/attributes.json')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    json_obj = response['Body'].read().decode('utf-8')
    assert 'n5' in json_obj


def test_prefixed_get_object(app, s3_client):
    response = s3_client.get_object(Bucket='janelia-data-examples', Key='jrc_mus_lung_covid.n5/attributes.json')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    json_obj = response['Body'].read().decode('utf-8')
    assert 'n5' in json_obj


def test_virtual_host_get_object(app, s3_client):
    response = s3_client.get_object(Bucket='janelia-data-examples', Key='jrc_mus_lung_covid.n5/attributes.json')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    json_obj = response['Body'].read().decode('utf-8')
    assert 'n5' in json_obj


def test_prefixed_list_objects(app, s3_client):
    response = s3_client.list_objects_v2(Bucket='janelia-data-examples', Prefix='jrc_mus_lung_covid.n5/', Delimiter='/')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert response['Name'] == 'janelia-data-examples'
    assert response['Delimiter'] == '/'
    assert len(response['CommonPrefixes']) == 2
    assert response['IsTruncated'] is False


def test_get_object_missing(app, s3_client):
    with pytest.raises(s3_client.exceptions.ClientError) as exc_info:
        s3_client.get_object(Bucket='janelia-data-examples', Key='missing')
    assert exc_info.value.response['Error']['Code'] == 'NoSuchKey'


def test_bucket_missing(app, s3_client):
    with pytest.raises(s3_client.exceptions.ClientError) as exc_info:
        s3_client.get_object(Bucket='missing', Key='attributes.json')
    assert exc_info.value.response['Error']['Code'] == 'NoSuchBucket'


def test_get_object_precedence(app, s3_client):
    response = s3_client.get_object(Bucket='janelia-data-examples', Key='jrc_mus_lung_covid.n5/attributes.json')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    json_obj = response['Body'].read().decode('utf-8')
    assert 'n5' in json_obj
