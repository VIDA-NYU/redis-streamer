#!/bin/bash
PID=`ps -fC redis-server | grep :6789 | cut -f 2- -d' ' | xargs | cut -d' ' -f 1`
if [ -z "$PID" ]
then
    echo "No redis-server running on port 6789"
else
    echo "Send SIGTERM to $PID"
    kill $PID
fi

