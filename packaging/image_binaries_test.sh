#!/usr/bin/env bash
# Checks the Connect worker image layer and manifest without a Docker daemon.
#
# The test asserts these properties of the built artifacts:
#
#   * the layer puts exactly `krabka-connect-worker` under /usr/bin,
#   * the binary runs and answers `--help`,
#   * the image manifest references the layer,
#   * the binary is an ELF for the architecture that the manifest declares.
#
# Argument 1 is the manifest architecture (IMAGE_ARCH in //packaging:BUILD.bazel).
# Argument 2 is the image manifest JSON. Argument 3 is the layer tarball.
set -euo pipefail

expected="krabka-connect-worker"

arch="$1"
manifest="$2"
layer="$3"

fail() {
    echo "image_binaries_test: $*" >&2
    exit 1
}

# `e_machine` is the two bytes at offset 0x12 of an ELF header, little-endian.
# EM_X86_64 is 62 (0x003e). EM_AARCH64 is 183 (0x00b7).
want_machine=""
case "${arch}" in
    amd64) want_machine="3e00" ;;
    arm64) want_machine="b700" ;;
    *) fail "no ELF e_machine known for architecture ${arch}" ;;
esac

elf_machine() {
    if [[ "$(od -An -tx1 -N 4 "$1" | tr -d ' \n')" != "7f454c46" ]]; then
        return 0
    fi
    od -An -tx1 -j 18 -N 2 "$1" | tr -d ' \n'
}

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d' ' -f1
    else
        shasum -a 256 "$1" | cut -d' ' -f1
    fi
}

work="${TEST_TMPDIR:-$(mktemp -d)}"
rootfs="${work}/rootfs"
mkdir -p "${rootfs}"

[[ -f "${manifest}" ]] || fail "image manifest ${manifest} is not a file"
[[ -f "${layer}" ]] || fail "layer ${layer} is not a file"
tar -xf "${layer}" -C "${rootfs}"

# rules_img stores a layer as the gzipped tarball that the rule writes, so the
# blob digest in the manifest is the digest of that file.
digest="$(sha256 "${layer}")"
grep -qF "sha256:${digest}" "${manifest}" ||
    fail "the image does not reference layer ${layer} (sha256:${digest})"

shipped="$(find "${rootfs}/usr/bin" -type f -exec basename {} \; | LC_ALL=C sort | tr '\n' ' ')"
[[ "${shipped}" == "${expected} " ]] ||
    fail "/usr/bin holds [${shipped}], expected [${expected} ]"

path="${rootfs}/usr/bin/${expected}"
[[ -x "${path}" ]] || fail "${expected} is not executable in the image"

found_machine="$(elf_machine "${path}")"
[[ "${found_machine}" == "${want_machine}" ]] ||
    fail "/usr/bin/${expected} has e_machine [${found_machine}], but the image manifest declares ${arch}"

status=0
"${path}" --help >/dev/null 2>&1 || status=$?
[[ "${status}" -eq 0 ]] || fail "${expected} --help exited ${status}"

echo "image_binaries_test: /usr/bin/${expected} is an ${arch} ELF and answers --help"
