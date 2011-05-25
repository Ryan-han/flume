#!/usr/bin/env bash

JAVA_HOME=/usr/java/default
DAEMON_HOME=/usr/bin
DAEMON_USER=root

PID_FILE=/tmp/rollmgr.pid
NWITTER_HOME=/mnt/icube-sc/flume

CLASSPATH=""
for f in $NWITTER_HOME/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

case "$1" in
    start)
	#
    # Start PostMan
    #
    $DAEMON_HOME/jsvc \
    -user $DAEMON_USER \
    -home $JAVA_HOME \
    -wait 10 \
    -pidfile $PID_FILE \
    -outfile $NWITTER_HOME/logs/rollingMgr.out \
    -errfile '&1' \
    -cp $CLASSPATH \
    com.nexr.rolling.core.RollingManagerImpl
    #
    # To get a verbose JVM
    #-verbose \
    # To get a debug of jsvc.
    #-debug \
    exit $?
    ;;
 
  stop)
    #
    # Stop PostMan
    #
    $DAEMON_HOME/jsvc \
    -stop \
    -pidfile $PID_FILE \
    com.nexr.rolling.core.RollingManagerImpl
    exit $?
    ;;
#
  *)
    echo "Usage rollingmanager.sh.sh start/stop"
    exit 1;;
esac
