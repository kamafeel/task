#!/bin/bash

if [ $# -ne 1 ];then
echo "input program args error... exit! "
exit -1
fi

find ./agent-logs/outlog/ -name "*.log" -type f -mtime +$1 -exec rm {} \;
