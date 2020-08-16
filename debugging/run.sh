#!/bin/bash
set -e

if [ ! -f ~/.druid_gcp_credentials.json ] ; then
  echo "Please create ~/.druid_gcp_credentials.json"
else
  export GOOGLE_APPLICATION_CREDENTIALS=~/.druid_gcp_credentials.json
  ../../builds/druid/bin/start-single-server-small
fi

