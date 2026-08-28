# Configuration

The `config.yaml` file configures the service. You can specify the following properties:

* `log_level`: The logging level (ERROR, WARNING, INFO, DEBUG, TRACE)
* `log_format`: `text` (default) for human-readable logs, or `json` for structured logs (see [Structured Logging](StructuredLogging.md))
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
    buffer_size: 65536  # 64 KB chunks for streaming
    calculate_etags: false
```

## Targets

Each target may have the following properties:

* `name`: Name of the bucket
* `browseable`: If `false`, the bucket becomes key-only: it is hidden from the main listing, all list/browse/ACL requests return `403 AccessDenied`, and objects can only be retrieved by exact key.
* `options`: Dictionary of client-specific options (see below)
* `client`: The client to use to access the storage location target. Supported clients:
    * *aioboto*: S3-compatible targets. Options:
        * `bucket`: Name of the S3 bucket
        * `prefix`: Prefix path into the storage
        * `endpoint`: URI of the S3 endpoint to use
        * `access_key_path`: Path to the S3 access key (for private buckets)
        * `secret_key_path`: Path to the S3 secret key (for private buckets)
        * `config`: Botocore configuration options (see below)
        * `proxy_etag`: If true, the backend's `ETag` is proxied through to clients (default: false). Most S3-compatible backends (e.g. VAST) return an `ETag` that is not a true content MD5, which causes the AWS CLI/SDK download integrity check to fail with "Unable to verify integrity of data download". Only set this to true for backends known to return a real content-MD5 `ETag` (e.g. real AWS S3).
    * *file*: Local filesystem targets. Options:
        * `path`: Path to the root
        * `buffer_size`: Size of chunks (in bytes) when streaming file content (default: 8192)
        * `calculate_etags`: If true, then the etags will be calculated by hashing the content of each file. This is much more expensive and may not be needed for all use cases.
        * `virtual_prefix`: Mounts the target at this key prefix inside the bucket. The prefix does not exist on disk: keys are reported with it, and it is stripped from incoming keys. This is the inverse of the aioboto `prefix` option, which hides a prefix that does exist upstream. For example, `virtual_prefix: shared/my-data` exposes `<root>/README.md` as `shared/my-data/README.md`.

### Botocore Config Options

The `config` option for aioboto targets accepts any [botocore Config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html) parameter. Common options include:

* `max_pool_connections`: Maximum connections in the pool (default: 30)
* `connect_timeout`: Timeout for connection attempts in seconds
* `read_timeout`: Timeout for read operations in seconds
* `retries`: Retry configuration dict (e.g., `{max_attempts: 3}`)
* `proxies`: Proxy configuration dict

Example:
```yaml
config:
  max_pool_connections: 50
  connect_timeout: 10
  read_timeout: 30
  retries:
    max_attempts: 3
```

## Example Configuration

```yaml
log_level: INFO
ui: true

# Global defaults for all clients of each type
client_options:
  aioboto:
    config:
      max_pool_connections: 50
      connect_timeout: 10
      read_timeout: 30

targets:
  # Public S3 bucket using global defaults
  - name: public-data
    client: aioboto
    options:
      bucket: my-public-bucket
      endpoint: https://s3.amazonaws.com

  # Private S3 bucket with custom config (overrides global defaults)
  - name: private-data
    client: aioboto
    options:
      bucket: my-private-bucket
      endpoint: https://s3.amazonaws.com
      access_key_path: /var/x2s3/access_key
      secret_key_path: /var/x2s3/secret_key
      config:
        max_pool_connections: 100
        retries:
          max_attempts: 5

  # Local filesystem
  - name: local-files
    client: file
    options:
      path: /data/files

  # Local filesystem with larger buffer for better throughput
  - name: large-files
    client: file
    options:
      path: /data/large-files
      buffer_size: 131072  # 128 KB chunks
```

## Notes

For each bucket, you can either provide credentials, or it will fallback on anonymous access. Credentials are read from files on disk. You can specify a `prefix` to constrain browsing of a bucket to a given subpath. Set `browseable: false` to make a bucket key-only: it is hidden from the main listing, all listing and browsing requests return `403 AccessDenied`, and `GET`/`HEAD` of a missing key returns `403` instead of `404` so that key existence cannot be probed. Objects are accessible only to clients that know the exact key. This mirrors AWS S3 behavior for a bucket policy that grants `s3:GetObject` but denies `s3:ListBucket`.

The `base_url` is how your server will be addressed externally. If you are using https then you will need to provide the `ssl-keyfile` and `ssl-certfile` when running Uvicorn (or equivalently `KEY_FILE` and `CERT_FILE` when running in Docker.)