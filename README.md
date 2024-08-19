# x2s3

![Python CI](https://github.com/JaneliaSciComp/x2s3/actions/workflows/python-ci.yml/badge.svg)

Proxy service which makes storage systems available with an S3-compatible REST API. It was built to support cloud-compatible viewers such as [N5 Viewer](https://github.com/saalfeldlab/n5-viewer) (BigDataViewer) and [Neuroglancer](https://github.com/google/neuroglancer).

<p align="center">
    <img src="https://raw.githubusercontent.com/JaneliaSciComp/x2s3/main/docs/use_cases.png">
</p>

Features:
* Extensible support for backend storage systems
* Web-based file browser
* Hidden buckets
* Hidden prefixes
* Object streaming

Inspired by S3 proxies such as [oxyno-zeta/s3-proxy](https://github.com/oxyno-zeta/s3-proxy) and [pottava/aws-s3-proxy](https://github.com/pottava/aws-s3-proxy), this service implements enough of the AWS S3 HTTP API to be useable by AWS clients, such as the one in BigDataViewer.

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
* etc.

# Getting Started

### Install dependencies

Create a virtualenv and install the dependencies:

    virtualenv env
    source env/bin/activate
    pip install -r requirements.txt

## Create target bucket configuration

Create a `config.yaml` file that contains all of the buckets you want to serve. You can get
started quickly by using the provided example template:

```bash
cp config.template.yaml config.yaml
```

For each bucket, you can either provide credentials, or it will fallback on anonymous access. Credentials are read from files on disk. You can specify a `prefix` to constrain browsing of a bucket to a given subpath. Set `hidden` to hide the bucket from the main listing -- you may also want to obfuscate the bucket name.

The `base_url` is how your server will be addressed externally. If you are using https then you will need to provide the `ssl-keyfile` and `ssl-certfile` when running Uvicorn (or equivalently `KEY_FILE` and `CERT_FILE` when running in Docker.)

## Run server

The service is written using FastAPI and runs inside of Uvicorn:

```bash
uvicorn x2s3.app:app --host 0.0.0.0 --port 8000 --access-log --reload
```

You can specify TLS certificates and increase the number of workers in order to scale the service for production usage:

```bash
uvicorn x2s3.app:app --host 0.0.0.0 --port 8000 --workers 8 --access-log --ssl-keyfile /opt/tls/cert.key --ssl-certfile /opt/tls/cert.crt
```

# Production Deployment

## Running inside a Docker container

First you'll need a `config.yaml` as described above.

Next, create a `./docker/.env` file that looks like this:

```bash
CONFIG_FILE=/path/to/config.yaml
VAR_DIR=/path/to/var/dir
CERT_FILE=/path/to/cert.crt
KEY_FILE=/path/to/cert.key
NGINX_CACHE_DIR=/path/to/cache
```

These properties configure the service as follows:
* `CONFIG_FILE`: path to the `config.yaml` settings file
* `VAR_DIR`: optional path to the var directory containing access keys referenced by `config.yaml`
* `CERT_FILE`: optional path to the SSL cert file
* `KEY_FILE`: optional path to the SSL key file
* `NGINX_CACHE_DIR`: path for Nginx response caching (disable this by editing `nginx.conf`)

Now you can bring up the container:

```bash
cd docker/
docker compose up -d
```

# Development Notes

## Testing

To run the tests and produce a code coverage report:

```bash
python -m pytest --cov=x2s3 --cov-report html -W ignore::DeprecationWarning
```

## Building the Docker container

Run the Docker build, replacing $VERSION with your version number:

```bash
cd docker/
docker build . --build-arg GIT_TAG=$VERSION -t ghcr.io/janeliascicomp/x2s3:$VERSION
```

## Pushing the Docker container

```bash
docker push ghcr.io/janeliascicomp/x2s3:$VERSION
```

# Attributions

Proxy icons created by <a href="https://www.flaticon.com/free-icons/proxy" title="proxy icons">Uniconlabs - Flaticon</a>
