#!/bin/sh
ppid=`ps -fe|grep "dacp-task-server-deploy-3.0.0.jar $1" |grep -v grep`
if [ ! -n "$ppid" ];then
	cd ../
	java -Dfile.encoding=UTF-8 -Xmx4096m -Xms4096m -jar dacp-task-server-deploy-3.0.0.jar $@ &
	echo "dacp-task-server-deploy-3.0.0.jar $@ started successfully......"
else
 	echo "dacp-task-server-deploy-3.0.0.jar $@ is running....."
fi