#!/bin/bash -e

#
# Script to upload tarball of assembly build to static.druid.io for serving
#
s3cmd put services/target/druid-services-*-bin.tar.gz s3://static.druid.io/artifacts/releases/
