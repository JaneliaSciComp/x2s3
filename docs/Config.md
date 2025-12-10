# Configuration

The `config.yaml` file configures the service. You can specify the following properties:

* `log_level`: The logging level (ERROR, WARNING, INFO, DEBUG, TRACE)
* `ui`: By default, the root shows an HTML UI listing of the buckets, with navigation. This disables the UI and restores the [ListBuckets](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html) functionality at the root.
* `virtual_buckets`: If true, then the buckets can be browsed like subdomains of the base URL, like 'https://bucketname.yourdomain.org'. This requires wildcard SSL certificates and additional configuration at the Nginx level, and requires that the `base_url` is set.
* `base_url`: The base URL for your service. Only needed when using `virtual_buckets`.
* `client_options`: Global default options for each client type (see below)
* `targets`: Ordered list of storage location targets to serve.

## Client Options

The `client_options` setting allows you to specify global default options for each client type. These options are merged with target-specific options, with target options taking precedence.

```yaml
client_options:
  aioboto:
    max_pool_connections: 50
  file:
    calculate_etags: false
```

## Targets

Each target may have the following properties:

* `name`: Name of the bucket
* `browseable`: Can this bucket be listed and browsed interactively?
* `options`: Dictionary of client-specific options (see below)
* `client`: The client to use to access the storage location target. Supported clients:
    * *aioboto*: S3-compatible targets. Options:
        * `bucket`: Name of the S3 bucket
        * `prefix`: Prefix path into the storage
        * `endpoint`: URI of the S3 endpoint to use
        * `access_key_path`: Path to the S3 access key (for private buckets)
        * `secret_key_path`: Path to the S3 secret key (for private buckets)
        * `max_pool_connections`: Maximum number of connections in the pool (default: 30)
    * *file*: Local filesystem targets. Options:
        * `path`: Path to the root
        * `calculate_etags`: If true, then the etags will be calculated by hashing the content of each file. This is much more expensive and may not be needed for all use cases.

## Example Configuration

```yaml
log_level: INFO
ui: true

# Global defaults for all clients of each type
client_options:
  aioboto:
    max_pool_connections: 50

targets:
  # Public S3 bucket with default pool size from client_options
  - name: public-data
    client: aioboto
    options:
      bucket: my-public-bucket
      endpoint: https://s3.amazonaws.com

  # Private S3 bucket with custom pool size (overrides global default)
  - name: private-data
    client: aioboto
    options:
      bucket: my-private-bucket
      endpoint: https://s3.amazonaws.com
      access_key_path: /var/x2s3/access_key
      secret_key_path: /var/x2s3/secret_key
      max_pool_connections: 100

  # Local filesystem
  - name: local-files
    client: file
    options:
      path: /data/files
```

## Notes

For each bucket, you can either provide credentials, or it will fallback on anonymous access. Credentials are read from files on disk. You can specify a `prefix` to constrain browsing of a bucket to a given subpath. Set `browseable: false` to hide the bucket from the main listing -- you may also want to obfuscate the bucket name.

The `base_url` is how your server will be addressed externally. If you are using https then you will need to provide the `ssl-keyfile` and `ssl-certfile` when running Uvicorn (or equivalently `KEY_FILE` and `CERT_FILE` when running in Docker.)