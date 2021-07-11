#!/bin/bash
set -ex

# Use project's Python virtualenv
source activate
which python

# Create new env with same Python version as project
virtualenv build-env
deactivate # get out of project's virutalenv sourced above
source build-env/bin/activate
which python
pip install -U setuptools wheel setuptools-rust
python setup.py bdist_wheel

# Cleanup
rm -rf build/*
rm -rf build-env