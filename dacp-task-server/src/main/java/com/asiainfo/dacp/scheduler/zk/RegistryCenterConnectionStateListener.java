package com.asiainfo.dacp.scheduler.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.asiainfo.dacp.scheduler.service.TaskProcessService;

/**
 * ZK连接状态监听器.
 *
 * @author zhangqi
 */
@Component
public final class RegistryCenterConnectionStateListener implements ConnectionStateListener {
    
	private static Logger LOG = LoggerFactory.getLogger(RegistryCenterConnectionStateListener.class);
	
	@Autowired
	private TaskProcessService tps;
    
    @Override
    public void stateChanged(final CuratorFramework client, final ConnectionState newState) {
    	LOG.info("Zk ConnectionState is changged,newState is {}",newState.name());
        if (newState.isConnected()) {
        	tps.setZkEnvNormal(true);
        	if(!tps.dpReceiver.isStart()){
        		tps.dpReceiver.start();
        	}
        }else {
        	tps.setZkEnvNormal(false);
        	/** zk链接断掉后停止本线程 **/
        	tps.setZkLeader(false);
        	LOG.info("Zk ConnectionState is lost,stop leadership");
        	/** zk链接断掉后停止本线程 **/
        	
        	if(tps.dpReceiver.isStart()){
        		tps.dpReceiver.stop();
        	}
        }
    }
}
