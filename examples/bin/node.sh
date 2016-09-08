#!/bin/bash -eu

## Initialization script for druid nodes
## Runs druid nodes as a daemon
## Environment Variables used by this script -
## DRUID_LIB_DIR - directory having druid jar files, default=lib
## DRUID_CONF_DIR - directory having druid config files, default=conf/druid
## DRUID_LOG_DIR - directory used to store druid logs, default=log
## DRUID_PID_DIR - directory used to store pid files, default=var/druid/pids

usage="Usage: node.sh nodeType (start|stop|status)"

if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

nodeType=$1
shift

command=$1

LIB_DIR="${DRUID_LIB_DIR:=lib}"
CONF_DIR="${DRUID_CONF_DIR:=conf/druid}"
LOG_DIR="${DRUID_LOG_DIR:=log}"
PID_DIR="${DRUID_PID_DIR:=var/druid/pids}"

pid=$PID_DIR/$nodeType.pid

case $command in

  (start)

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $nodeType node running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    nohup java `cat $CONF_DIR/$nodeType/jvm.config | xargs` -cp $CONF_DIR/_common:$CONF_DIR/$nodeType:$LIB_DIR/* io.druid.cli.Main server $nodeType > $LOG_DIR/$nodeType.log &
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

   (status)
    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo RUNNING
        exit 0
      else
        echo STOPPED
      fi
    else
      echo STOPPED
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;
esac
