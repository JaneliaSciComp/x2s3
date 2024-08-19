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

See the [documentation](docs/Config.md) for more information about the configuration file.


## Run server

The service is written using FastAPI and runs inside of Uvicorn. You can start a dev server quickly with the `run.py` script:

```bash
./run.py --port 8000
```

This is equivalent to:

```bash
uvicorn x2s3.app:app --host 0.0.0.0 --port 8000 --workers 1 --access-log --reload
```

You can specify TLS certificates and increase the number of workers in order to scale the service for production usage:

```bash
uvicorn x2s3.app:app --host 0.0.0.0 --port 8000 --workers 8 --access-log --ssl-keyfile /opt/tls/cert.key --ssl-certfile /opt/tls/cert.crt
```

# Production Deployment

## Running inside a Docker container

For production deployments, we recommend using an orchestrator (like Docker Compose) to run the prebuilt Docker container along with an Nginx reverse proxy which provides caching and TLS termination.

Create a `./docker/.env` file that looks like this:

```bash
CONFIG_FILE=/path/to/config.yaml
VAR_DIR=/path/to/var/dir
CERT_DIR=/path/to/certs
NGINX_CACHE_DIR=/path/to/cache
```

These properties configure the service as follows:
* `CONFIG_FILE`: path to the `config.yaml` settings file
* `VAR_DIR`: optional path to the var directory containing access keys referenced by `config.yaml`
* `CERT_FILE`: optional path to the SSL cert file
* `KEY_FILE`: optional path to the SSL key file
* `NGINX_CACHE_DIR`: path for Nginx response caching (you can disable caching by editing `nginx.conf`)

Now you can bring up the container:

```bash
cd docker/
docker compose up -d
```

# Documentation

See the [documentation](docs/Development.md) for more information about development.


# Attributions

Proxy icons created by <a href="https://www.flaticon.com/free-icons/proxy" title="proxy icons">Uniconlabs - Flaticon</a>
