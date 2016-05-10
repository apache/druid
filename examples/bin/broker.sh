#!/bin/bash -eu

usage="Usage: broker.sh (start|stop)"

if [ $# -lt 1 ]; then
  echo $usage
  exit 1
fi

sh ./bin/node.sh broker $1
