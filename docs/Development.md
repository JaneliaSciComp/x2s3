# Development Notes

## Getting Started

You will need [Pixi](https://pixi.sh/latest/) to build this project.

```bash
pixi run dev-install
```

The service is written using FastAPI and runs inside of Uvicorn. You can start a dev server quickly with this command:

Then run the development server:

```bash
pixi run dev-launch
```

On a remote system, you may need SSL certs (e.g. for loading into Neuroglancer), which can be added like this (assuming your certificate is found at `/opt/certs/cert.key` and `/opt/certs/cert.crt`):

```bash
pixi run dev-launch-remote
```

For production deployments, please refer to the main [README](../README.md) file.


## Testing

To run the unit tests and produce a code coverage report:

```bash
pixi run test
```

These tests are automatically run whenever changes are merged to the *main* branch.

## Release

First, increment the version in `pyproject.toml` and push it to GitHub. Create a *Release* there and then publish it to PyPI as follows.

To create a Python source package (`.tar.gz`) and the binary package (`.whl`) in the `dist/` directory, do:

```bash
pixi run pypi-build
```

To upload the package to the PyPI, you'll need one of the project owners to add you as a collaborator. After setting up your access token, do:

```bash
pixi run pypi-upload
```

### Building the Docker container

Run the Docker build and push to GHCR, replacing `<version>` with your version number:

```bash
cd docker/
export VERSION=<version>
docker buildx build --platform linux/amd64,linux/arm64 --build-arg GIT_TAG=$VERSION -t ghcr.io/janeliascicomp/x2s3:$VERSION -t ghcr.io/janeliascicomp/x2s3:latest --push .
```

If you are using Podman, you can do something like this:

```bash
cd docker/
export VERSION=<version>
export IMAGE="ghcr.io/janeliascicomp/x2s3"
podman build --jobs=2 --platform=linux/amd64,linux/arm64 \
      --manifest "$IMAGE:$VERSION" --tag "$IMAGE:latest" .
```

Push the images to GHCR:
```
podman manifest push --all "$IMAGE:$VERSION" "docker://$IMAGE:$VERSION"
podman manifest push --all "$IMAGE:latest" "docker://$IMAGE:latest"
```
