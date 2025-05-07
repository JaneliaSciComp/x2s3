# Development Notes

## Getting Started

Create a virtualenv and install the dependencies:

    virtualenv env
    source env/bin/activate
    pip install -r requirements.txt

The service is written using FastAPI and runs inside of Uvicorn. You can start a dev server quickly with the `run.sh` helper script:

```bash
./run.sh
```

This is equivalent to running Uvicorn directly, like this:

```bash
uvicorn x2s3.app:app --host 0.0.0.0 --port 8000 --workers 1 --access-log --reload
```

You can specify TLS certificates and increase the number of workers in order to scale the service:

```bash
uvicorn x2s3.app:app --host 0.0.0.0 --port 8000 --workers 8 --access-log --ssl-keyfile /opt/tls/cert.key --ssl-certfile /opt/tls/cert.crt
```

For production deployments, please refer to the main [README](../README.md) file.


## Testing

To run the unit tests and produce a code coverage report:

```bash
python -m pytest --cov=x2s3 --cov-report html -W ignore::DeprecationWarning
```

These tests are automatically run whenever changes are merged to the *main* branch.


## Building the Docker container

Run the Docker build, replacing `<version>` with your version number:

```bash
cd docker/
export VERSION=<version>
docker buildx build --platform linux/amd64,linux/arm64 --build-arg GIT_TAG=$VERSION -t ghcr.io/janeliascicomp/x2s3:$VERSION -t ghcr.io/janeliascicomp/x2s3:latest --push .
```

## Deploying to PyPI

After creating a new release, remember to update the version in `pyproject.toml`, then:

```
pip install build twine
python -m build
python -m twine upload dist/*
```
