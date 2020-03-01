package com.asiainfo.dacp.zk;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.ai.zq.common.reg.base.CoordinatorRegistryCenter;
import com.ai.zq.common.reg.zookeeper.ZookeeperConfiguration;
import com.ai.zq.common.reg.zookeeper.ZookeeperRegistryCenter;

/**
 * ZK操作类型Agent端
 * @author zhangqi
 *
 */
@Component
public class AgentZkOperation {
	
	private CoordinatorRegistryCenter regCenter;
	
	@Autowired
	private RegistryCenterConnectionStateListener rccsl;
	
	private static final String AGENT_HOST_NODE = "agent/instance";
	private static final String AGENT_RUN_NODE = "agent/run";
	
	@Value("${zq.zk.cfg.serverLists}")
	private String serverLists;
	@Value("${zq.zk.cfg.namespace}")
	private String namespace;
	@Value("${zq.zk.cfg.sessionTimeoutMilliseconds}")
	private int sessionTimeoutMilliseconds;
	@Value("${zq.zk.cfg.connectionTimeoutMilliseconds}")
	private int connectionTimeoutMilliseconds;
	@Value("${zq.zk.cfg.digest}")
	private String digest;
	
	private ZookeeperConfiguration setUpRegistryCenter(){
		ZookeeperConfiguration zkConfig = new ZookeeperConfiguration(getServerLists(), namespace);
		zkConfig.setSessionTimeoutMilliseconds(sessionTimeoutMilliseconds);
		zkConfig.setConnectionTimeoutMilliseconds(connectionTimeoutMilliseconds);
		if(!StringUtils.isEmpty(digest)){
			zkConfig.setDigest(digest);
		}
		return zkConfig;
	}
	
	private void registryListener() {
		this.addConnectionStateListener(rccsl);
	}
	
	private CuratorFramework getClient() {
        return (CuratorFramework) regCenter.getRawClient();
    }
	
	public AgentZkOperation init(){
		regCenter = new ZookeeperRegistryCenter(this.setUpRegistryCenter());
		regCenter.init();
		this.registryListener();
		return this;
	}
	
	private String getAgentPath(final String agentCode) {
        return String.format("/%s/%s", AGENT_HOST_NODE, agentCode);
    }
    
	private String getAgentRunPath(final String agentCode,final String msgId) {
        return String.format("/%s/%s/%s", AGENT_RUN_NODE, agentCode,msgId);
    }
	
	/**
     * 注册连接状态监听器.
     * 
     * @param listener 连接状态监听器
     */
    public void addConnectionStateListener(final ConnectionStateListener listener) {
        getClient().getConnectionStateListenable().addListener(listener);
    }
    
    
    public void fillEphemeralAgentNode(final String agentCode, final Object value) {
        regCenter.persistEphemeral(this.getAgentPath(agentCode), value.toString());
    }
    
    public void fillEphemeralAgentRunTime(final String agentCode, final String msgId, final Object value) {
        regCenter.persistEphemeral(this.getAgentRunPath(agentCode,msgId), value.toString());
    }
    
    /**
     * 判断Agent作业进程是否存在.
     * 
     */
    public boolean isAgentRunTimeExisted(final String agentCode,final String msgId) {
        return regCenter.isExisted(this.getAgentRunPath(agentCode,msgId));
    }
    
    /**
     * 删除Agent作业进程节点.
     * 
     */
    public void removeAgentRunTimeIfExisted(final String agentCode,final String msgId) {
        if (isAgentRunTimeExisted(agentCode, msgId)) {
            regCenter.remove(this.getAgentRunPath(agentCode,msgId));
        }
    }

	public String getServerLists() {
		return serverLists;
	}

	public void setServerLists(String serverLists) {
		this.serverLists = serverLists;
	}
    
}
