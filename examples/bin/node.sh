#!/bin/bash -eu

## Initializtion script for druid nodes
## Runs druid nodes as a daemon and pipes logs to log/ directory

usage="Usage: node.sh nodeType (start|stop)"

if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

nodeType=$1
shift

startStop=$1
pid=var/druid/pids/$nodeType.pid

case $startStop in

  (start)

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $nodeType node running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    nohup java `cat conf/druid/$nodeType/jvm.config | xargs` -cp conf/druid/_common:conf/druid/$nodeType:lib/* io.druid.cli.Main server $nodeType > log/$nodeType.log &
    nodeType_PID=$!
    echo $nodeType_PID > $pid
    echo "Started $nodeType node ($nodeType_PID)"
    ;;

  (stop)

    if [ -f $pid ]; then
      TARGET_PID=`cat $pid`
      if kill -0 $TARGET_PID > /dev/null 2>&1; then
        echo Stopping process `cat $pid`...
        kill $TARGET_PID
      else
        echo No $nodeType node to stop
      fi
      rm -f $pid
    else
      echo No $nodeType node to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;
esac
