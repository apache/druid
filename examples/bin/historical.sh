#!/bin/bash -eu

usage="Usage: historical.sh (start|stop)"

if [ $# -lt 1 ]; then
  echo $usage
  exit 1
fi

sh ./bin/node.sh historical $1
