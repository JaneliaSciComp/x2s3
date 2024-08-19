# Development Notes

## Testing

To run the tests and produce a code coverage report:

```bash
python -m pytest --cov=x2s3 --cov-report html -W ignore::DeprecationWarning
```

## Building the Docker container

Run the Docker build, replacing `<version>` with your version number:

```bash
cd docker/
export VERSION=<version>
docker build . --build-arg GIT_TAG=$VERSION -t ghcr.io/janeliascicomp/x2s3:$VERSION
```

## Pushing the Docker container

```bash
docker push ghcr.io/janeliascicomp/x2s3:$VERSION
```
