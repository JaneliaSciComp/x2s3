# x2s3

[![Python CI](https://github.com/JaneliaSciComp/x2s3/actions/workflows/python-ci.yml/badge.svg)](https://github.com/JaneliaSciComp/x2s3/actions/workflows/python-ci.yml)

RESTful web service which makes any storage system *X* available as an S3-compatible REST API, hence the name "X to S3". It was initially built to support cloud-compatible microscopy image viewers such as [N5 Viewer](https://github.com/saalfeldlab/n5-viewer) (BigDataViewer) and [Neuroglancer](https://github.com/google/neuroglancer).

At Janelia, we use **x2s3** to make private buckets on Seagate Lyve appear public, and to proxy internal resources (e.g. VAST S3). It can also be used as a pop-up file service for quickly viewing local images in BigDataViewer or Neuroglancer.

<p align="center">
    <img src="https://raw.githubusercontent.com/JaneliaSciComp/x2s3/main/docs/use_cases.png">
</p>

# Features

* Extensible support for backend storage systems
* Optional web-based bucket explorer
* Hidden buckets
* Partial buckets (chroot-like prefixes)
* Non-blocking object streaming

Inspired by S3 proxies such as [oxyno-zeta/s3-proxy](https://github.com/oxyno-zeta/s3-proxy) and [pottava/aws-s3-proxy](https://github.com/pottava/aws-s3-proxy), this service goes a step further to implement enough of the [AWS S3 API](https://docs.aws.amazon.com/AmazonS3/latest/API/Type_API_Reference.html) to be useable by AWS clients, such as  BigDataViewer. The S3 proxy implements which do this well (e.g. [gaul/s3proxy](https://github.com/gaul/s3proxy)) only proxy a single bucket at a time.

S3 endpoints implemented:
* [GetBucketAcl](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAcl.html)
* [GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)
* [HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)
* [ListBuckets](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html)
* [ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)

S3 features omitted:
* Permissions
* Encryption
* Versioning
* RequestPayer
* etc.

# Running

Create a `config.yaml` file that contains all of the buckets you want to serve. You can get started quickly by using the provided example template:

```bash
cp config.template.yaml config.yaml
```

See the [documentation](docs/Config.md) for more information about the configuration file.

The simplest way to run the service is to use Docker:

```bash
docker run -it -p 8000:8000 -v ./config.yaml:/app/x2s3/config.yaml ghcr.io/janeliascicomp/x2s3:latest
```

You can also run the service with Python/Uvicorn directly. See the [development documentation](docs/Development.md) for more information on setting that up.


## Production Deployment

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

# Additional Documentation

* [Configuation](docs/Config.md) - how to use `config.yaml` to configure the service
* [Development](docs/Development.md) - notes on developing the service codebase


# Attributions

* Proxy icons created by [Uniconlabs - Flaticon](https://www.flaticon.com/free-icons/proxy)
* [AWS S3 API](https://docs.aws.amazon.com/AmazonS3/latest/API/Type_API_Reference.html) Reference
