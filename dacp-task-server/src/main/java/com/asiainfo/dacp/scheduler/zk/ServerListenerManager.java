package com.asiainfo.dacp.scheduler.zk;

import java.util.List;

import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ai.zq.common.listener.AbstractJobListener;
import com.ai.zq.common.listener.AbstractListenerManager;
import com.ai.zq.common.reg.base.CoordinatorRegistryCenter;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbStorage;
import com.asiainfo.dacp.scheduler.service.TaskProcessService;

/**
 * Server监听管理器.
 * 
 * @author zhangqi
 */
public class ServerListenerManager extends AbstractListenerManager {
    
	private static final String SEVER_RUN_NODE = "/server/run";
	
	private Logger LOG = LoggerFactory.getLogger(ServerListenerManager.class);
	
	private TaskProcessService tps;
	private TaskEventRdbStorage storage;
	private String serverLeader;
	
    public ServerListenerManager(final CoordinatorRegistryCenter regCenter,TaskProcessService tps,TaskEventRdbStorage storage,String serverLeader) {
        super(regCenter);
        this.tps = tps;
    	this.storage = storage;
    	this.serverLeader = serverLeader;
    }
    
    @Override
    public void start() {
    	regCenter.addCacheData(SEVER_RUN_NODE);
        addDataListener(new ServerRunTimeListener(),SEVER_RUN_NODE);
    }
    
    
    class ServerRunTimeListener extends AbstractJobListener {
    	
        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
        	if(!tps.isZkEnvNormal() || !tps.isZkLeader()){
        		return;
        	}
        	if(path.equalsIgnoreCase(SEVER_RUN_NODE)){
        		List<String> servers = regCenter.getChildrenKeys(path);
        		LOG.info("Server[{}] status is changge",servers.toString());
        		storage.updateServerStatus(servers, serverLeader);
        	}
        }
    }
}
