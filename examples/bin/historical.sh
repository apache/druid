#!/bin/bash -eu

usage="Usage: historical.sh (start|stop|status)"

if [ $# -lt 1 ]; then
  echo $usage
  exit 1
fi

cd $(dirname $0)/../
sh ./bin/node.sh historical $1
