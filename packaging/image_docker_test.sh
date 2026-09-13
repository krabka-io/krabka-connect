#!/usr/bin/env bash
# Loads the Connect worker image into a Docker daemon and runs it.
#
# //packaging:image_binaries_test runs the binary on the host. This test runs it
# in a container, under the glibc and the loader of the apko base. It finds a
# binary that links against a library the base does not carry.
#
#     bazel test -c opt --config=docker //packaging:image_docker_test
#
# Argument 1 is the `image_load` runner. Argument 2 is the tag it loads.
set -euo pipefail

loader="$1"
tag="$2"

if ! docker info >/dev/null 2>&1; then
    echo "image_docker_test: no reachable Docker daemon." >&2
    echo "  Start Docker, or run //packaging:image_binaries_test, which checks" >&2
    echo "  the layer without a daemon." >&2
    exit 1
fi

"${loader}"

# The entrypoint of the image, with no `--entrypoint` override. This is what a
# bare `docker run` starts.
if ! output="$(docker run --rm "${tag}" --help)"; then
    echo "image_docker_test: the entrypoint of ${tag} did not answer --help" >&2
    exit 1
fi

if [[ "${output}" != *"--kafka-bootstrap"* ]]; then
    echo "image_docker_test: the --help output of ${tag} is not the worker's:" >&2
    echo "${output}" >&2
    exit 1
fi

user="$(docker image inspect --format '{{.Config.User}}' "${tag}")"
if [[ "${user}" != "65532:65532" ]]; then
    echo "image_docker_test: ${tag} runs as [${user}], expected [65532:65532]" >&2
    exit 1
fi

echo "image_docker_test: ${tag} starts krabka-connect-worker as 65532:65532"
