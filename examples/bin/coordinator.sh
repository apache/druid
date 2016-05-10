#!/bin/bash -eu

usage="Usage: coordinator.sh (start|stop)"

if [ $# -lt 1 ]; then
  echo $usage
  exit 1
fi

sh ./bin/node.sh coordinator $1
