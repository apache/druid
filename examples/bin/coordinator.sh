#!/bin/bash -eu

usage="Usage: coordinator.sh (start|stop|status)"

if [ $# -lt 1 ]; then
  echo $usage
  exit 1
fi

sh ./bin/node.sh coordinator $1
