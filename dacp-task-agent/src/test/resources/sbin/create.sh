#!/bin/bash

if [ $# -ne 5 ];then

echo "input program args error... exit! "

exit -1

fi

agent=$1
ip=$2
username=$3
password=$4
zkList=$5

agent_f="../config/${1}"
echo $agent_f

#create agent directory

if [ ! -d "${agent_f}" ];then
  mkdir -pv ${agent_f}
fi
if [ ! -d "../bin" ];then
  mkdir ../bin
fi

#copy and create file from template directory
rm -rf ${agent_f}/agentconfig.properties.tmp
rm -rf ${agent_f}/applicationContext.xml.tmp
rm -rf ../bin/start-${agent}.sh.tmp
rm -rf ../bin/stop-${agent}.sh.tmp

cp ../template/config/applicationContext.xml ${agent_f}/applicationContext.xml.tmp
cat ${agent_f}/applicationContext.xml.tmp | sed "s/\${agent}/${agent}/g" | sed "s/\${ip_address}/${ip}/g" | sed "s/\${username}/${username}/g" | sed "s/\${password}/${password}/g" >${agent_f}/applicationContext.xml

cp ../template/config/agentconfig.properties ${agent_f}/agentconfig.properties.tmp
cat ${agent_f}/agentconfig.properties.tmp | sed "s/\${agent}/${agent}/g" | sed "s/\${zkList}/${zkList}/g" > ${agent_f}/agentconfig.properties



cp ../template/bin/start-agent.sh ../bin/start-${agent}.sh.tmp

cat ../bin/start-${agent}.sh.tmp | sed "s/\${agent}/${agent}/g" >../bin/start-${agent}.sh

chmod +x ../bin/start-${agent}.sh

cp ../template/bin/stop-agent.sh  ../bin/stop-${agent}.sh.tmp

cat ../bin/stop-${agent}.sh.tmp | sed "s/\${agent}/${agent}/g" >../bin/stop-${agent}.sh

chmod +x ../bin/stop-${agent}.sh

rm -rf ${agent_f}/agentconfig.properties.tmp
rm -rf ${agent_f}/applicationContext.xml.tmp
rm -rf ../bin/start-${agent}.sh.tmp
rm -rf ../bin/stop-${agent}.sh.tmp