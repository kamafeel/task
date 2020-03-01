#!/bin/sh
ppid=`ps -fe|grep "dacp-task-server-deploy-3.0.0.jar $1" |grep -v grep`
if [ ! -n "$ppid" ];then
 echo "the dacp-task-server-deploy-3.0.0.jar $1 is stopped....."
else
 pid=`ps -ef|grep "dacp-task-server-deploy-3.0.0.jar $1" |grep -v grep|awk '{print $2}'`
  for id in $pid
  do
  echo $id
   kill -9 $id
   done
   echo "dacp-task-server-deploy-3.0.0.jar $1 killed successfully...."
fi