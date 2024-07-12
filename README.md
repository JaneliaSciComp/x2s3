# jproxy

This is a minimal proxy server to make data sets on S3-compatible interfaces (AWS S3, Seagate Lyve, VAST S3, etc.) available pubicly. It was built to support NGFF viewers such as [N5 Viewer](https://github.com/saalfeldlab/n5-viewer) (BigDataViewer) and [Neuroglancer](https://github.com/google/neuroglancer).

Features:
* HTML listings
* Mulltiple data sources
* Hidden buckets
* Hidden prefixes
* Object streaming

Inspired by other proxies such as [oxyno-zeta/s3-proxy](https://github.com/oxyno-zeta/s3-proxy) and [pottava/aws-s3-proxy](https://github.com/pottava/aws-s3-proxy), this proxy also implements enough of the AWS S3 HTTP API to be useable by AWS clients, such as the one in BigDataViewer.

S3 endpoints implemented:
* [GetBucketAcl](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAcl.html)
* [HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)
* [GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)
* [ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)

S3 features omitted:
* Permissions
* Encryption
* Versioning
* RequestPayer

# Getting Started

### Install dependencies

Create a virtualenv and install the dependencies:

    virtualenv env
    source env/bin/activate
    pip install -r requirements.txt

## Create target bucket configuration

Create a `config.yaml` file that contains all of the buckets you want to proxy:

```yaml
targets:
  - name: scicompsoft-public
    endpoint: https://s3.us-east-1.lyvecloud.seagate.com/
    bucket: scicompsoft-public
    credentials:
      accessKeyPath: './var/access_key'
      secretKeyPath: './var/secret_key'

  - name: opendata
    endpoint: https://internal.hostname/
    bucket: opendata
    hidden: true

  - name: scicompsoft
    endpoint: https://internal.hostname/
    bucket: scicompsoft
    prefix: path/to/data
```

For each bucket, you can either provide credentials, or it will fallback on anonymous access. Credentials are read from files on disk. You can specify a `prefix` to constrain browsing of a bucket to a given subpath. Set `hidden` to hide the bucket from the main listing -- you may also want to obfuscate the bucket name.

## Run server

The service is written using FastAPI and runs inside of Uvicorn:

```bash
uvicorn jproxy.serve:app --host 0.0.0.0 --port 8000 --access-log --reload
```

You can specify TLS certificates and increase the number of workers in order to scale the service for production usage:

```bash
uvicorn jproxy.serve:app --host 0.0.0.0 --port 8000 --workers 8 --access-log --ssl-keyfile /opt/tls/cert.key --ssl-certfile /opt/tls/cert.crt
```

# Production

## Running inside a Docker container

First you'll need a `config.yaml` as described above.

Create a `./docker/.env` file that looks like this:

```bash
CONFIG_FILE=/path/to/config.yaml
VAR_DIR=/path/to/var
CERT_FILE=/path/to/cert.crt
KEY_FILE=/path/to/cert.key
```

Now you can bring up the container:

```bash
cd docker/
docker compose up -d
```

# Development Notes

## Building the Docker container

Run the Docker build, replacing `latest` with your version number:

```bash
cd docker/
docker build . --no-cache -t ghcr.io/janeliascicomp/jproxy:latest
```

## Pushing the Docker container

```bash
docker push ghcr.io/janeliascicomp/jproxy:latest
```

# Attributions

Proxy icons created by <a href="https://www.flaticon.com/free-icons/proxy" title="proxy icons">Uniconlabs - Flaticon</a>
