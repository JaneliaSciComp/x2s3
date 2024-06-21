# jproxy

This is a minimal proxy server to make data sets on S3-compatible interfaces (AWS S3, Seagate Lyve, VAST S3, etc.) available to NGFF viewers such as N5 Viewer and Neuroglancer.

## Create configuration

Create a `config.yaml` file that contains all of your buckets:

```yaml
targets:
  scicompsoft-public:
    endpoint: https://s3.us-east-1.lyvecloud.seagate.com/
    bucket: scicompsoft-public
    credentials:
      accessKeyPath: './var/access_key'
      secretKeyPath: './var/secret_key'

  opendata:
    endpoint: https://internal.hostname/
    bucket: opendata

  scicompsoft:
    endpoint: https://internal.hostname/
    bucket: scicompsoft
    prefix: path/to/data
```

You can either provide credentials, or it will fallback on anonymous access.


### Install dependencies

Create a virtualenv and install the dependencies:

    virtualenv env
    source env/bin/activate
    pip install -r requirements.txt


## Run

```bash
uvicorn serve:app --host 0.0.0.0 --port 8000 --workers 1 --access-log --ssl-keyfile /opt/tls/cert.key --ssl-certfile /opt/tls/cert.crt --reload
```

