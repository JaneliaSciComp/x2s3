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

  vast-nrs-opendata:
    endpoint: https://nrs.int.janelia.org/
    bucket: opendata

  vast-nrs-scicompsoft:
    endpoint: https://nrs.int.janelia.org/
    bucket: scicompsoft
```

You can either provide credentials, or it will fallback on anonymous access.


## Create environment

```bash
conda env create -f environment.yml -y
conda activate zarrcade
```

## Run

```bash
uvicorn serve:app --host 0.0.0.0 --port 8000 --workers 1 --access-log
```

