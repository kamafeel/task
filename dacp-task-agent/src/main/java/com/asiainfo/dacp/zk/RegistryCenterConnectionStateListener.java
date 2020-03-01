package com.asiainfo.dacp.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.asiainfo.dacp.dp.agent.DpAgentContext;

/**
 * 注册中心连接状态监听器.
 *
 * @author zhangqi
 */
@Component
public final class RegistryCenterConnectionStateListener implements ConnectionStateListener {
    
	private static Logger LOG = LoggerFactory.getLogger(RegistryCenterConnectionStateListener.class);
    
	@Autowired
	private DpAgentContext dc;
	
    @Override
    public void stateChanged(final CuratorFramework client, final ConnectionState newState) {
    	LOG.info("Zk ConnectionState is changged,newState is {}",newState.name());
        if (newState.isConnected()) {
        	dc.getAzko().fillEphemeralAgentNode(dc.getAgentCode(), "live");
        }
    }
}
