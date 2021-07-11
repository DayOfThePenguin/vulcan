#!/bin/bash
# Build multilinux wheels for multiple versions of Python.
# More advanced version of build-wheel.sh that lets you target multiple
# versisons of Python.
# ** Needs docker container ** See:
# https://github.com/PyO3/setuptools-rust#binary-wheels-on-linux

set -ex

curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain stable -y
export PATH="$HOME/.cargo/bin:$PATH"

cd /io

for PYBIN in /opt/python/{cp38-cp38,cp39-cp39}/bin; do
    "${PYBIN}/pip" install -U setuptools wheel setuptools-rust
    "${PYBIN}/python" setup.py bdist_wheel
done

for whl in dist/*.whl; do
    auditwheel repair "$whl" -w dist/
done

# Keep only manylinux wheels
rm dist/*-linux_*