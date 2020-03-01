package com.asiainfo.dacp.scheduler.zk;

import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.zq.common.listener.AbstractJobListener;
import com.ai.zq.common.listener.AbstractListenerManager;
import com.ai.zq.common.reg.base.CoordinatorRegistryCenter;
import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbStorage;
import com.asiainfo.dacp.scheduler.service.TaskProcessService;

/**
 * agent监听管理器.
 * 
 * @author zhangqi
 */

public class AgentListenerManager extends AbstractListenerManager {
    
	private static final String AGENT_HOST_NODE = "/agent/instance";
	private static final String AGENT_RUN_NODE = "/agent/run";
	private Logger LOG = LoggerFactory.getLogger(AgentListenerManager.class);
	
	private TaskProcessService tps;
	private TaskEventRdbStorage storage;
	
    public AgentListenerManager(final CoordinatorRegistryCenter regCenter,TaskProcessService tps,TaskEventRdbStorage storage) {
        super(regCenter);
    	this.tps = tps;
    	this.storage = storage;
    }
    
    @Override
    public void start() {
    	regCenter.addCacheData(AGENT_HOST_NODE);
    	regCenter.addCacheData(AGENT_RUN_NODE);
        addDataListener(new AgentRunTimeListener(),AGENT_RUN_NODE);
        addDataListener(new AgentInstanceListener(),AGENT_HOST_NODE);
    }
    
    
    class AgentRunTimeListener extends AbstractJobListener {
    	
        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
        	if(!tps.isZkEnvNormal() || !tps.isZkLeader()){
        		return;
        	}
        	if(path.matches("(?:" + AGENT_RUN_NODE + "/" + ").+")){
        		String agentName = path.split("/")[3];
        		if(MemCache.AGENT_IPS_MAP.containsKey(agentName)){
        			String path_ = AGENT_RUN_NODE + "/"  + agentName;
        			int crtips = regCenter.getNumChildren(path_);
        			MemCache.AGENT_IPS_MAP.get(agentName).setCurips(crtips);
        			LOG.info("[{}] agent of current ips is {}",path_,crtips);
        			storage.updateAgentInfo(agentName,String.valueOf(MemCache.AGENT_IPS_MAP.get(agentName).getAgentStatus()),String.valueOf(crtips));
        		}
        	}
        }
    }
    
    class AgentInstanceListener extends AbstractJobListener {
        
        @Override
		protected void dataChanged(final String path, final Type eventType, final String data) {
        	if(!tps.isZkEnvNormal() || !tps.isZkLeader()){
        		return;
        	}
        	
        	if(!path.matches("(?:" + AGENT_HOST_NODE + "/" + ").+")){
        		return;
        	}        	
        	String agentName = path.replaceFirst(AGENT_HOST_NODE + "/", "");
        	if(!MemCache.AGENT_IPS_MAP.containsKey(agentName)){
        		return;
        	}
        	if(Type.NODE_ADDED == eventType){
        		LOG.info("[{}] agent is online",path);
        		MemCache.AGENT_IPS_MAP.get(agentName).setAgentStatus(1);
        		//当前并发数不能设置为0,避免和AgentRunTimeListener冲突
        		storage.updateAgentInfo(agentName,"1",String.valueOf(MemCache.AGENT_IPS_MAP.get(agentName).getCurips()));
        	}else if(Type.NODE_REMOVED == eventType){
        		LOG.warn("[{}] agent is offline",path);
        		MemCache.AGENT_IPS_MAP.get(agentName).setAgentStatus(0);
        		storage.updateAgentInfo(agentName,"0",String.valueOf(MemCache.AGENT_IPS_MAP.get(agentName).getCurips()));
        	}
		}
    }
}
