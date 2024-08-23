# Configuration

The `config.yaml` file configures the service. 

* `base_url`: The base URL for your service. Only needed when using virtual-host-styler addressing, like 'https://bucketname.yourdomain.org'
* `log_level`: The logging level (ERROR, WARNING, INFO, DEBUG, TRACE)
* `ui`: By default, the root shows an HTML UI listing of the buckets, with navigation. This disables the UI and restores the [ListBuckets](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html) functionality at the root.
* `targets`: Ordered list of storage location targets to serve

Each target may have the following properties:

* `name`: Name of the bucket 
* `browseable`: Can this bucket be listed and browsed interactively?
* `options`: Dictionary of client-specific options (see below)
* `client`: The client to use to access the storage location target. Supported clients:
    * *aioboto*: S23-compatible targets. Options:
        * `prefix`: Prefix path into the storage 
        * `endpoint`: URI of the S3 endpoint to use
        * `access_key_path`: Path to the S3 access key (for private buckets)
        * `secret_key_path`: Path to the S3 secret key (for private buckets)
    * *local*: Local filesystem targets. Options:
        * `path`: Path to the root 

For each bucket, you can either provide credentials, or it will fallback on anonymous access. Credentials are read from files on disk. You can specify a `prefix` to constrain browsing of a bucket to a given subpath. Set `hidden` to hide the bucket from the main listing -- you may also want to obfuscate the bucket name.

The `base_url` is how your server will be addressed externally. If you are using https then you will need to provide the `ssl-keyfile` and `ssl-certfile` when running Uvicorn (or equivalently `KEY_FILE` and `CERT_FILE` when running in Docker.)