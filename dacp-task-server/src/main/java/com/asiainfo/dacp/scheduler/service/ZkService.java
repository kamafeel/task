package com.asiainfo.dacp.scheduler.service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.ai.zq.common.reg.base.CoordinatorRegistryCenter;
import com.ai.zq.common.reg.zookeeper.ZookeeperConfiguration;
import com.ai.zq.common.reg.zookeeper.ZookeeperElectionService;
import com.ai.zq.common.reg.zookeeper.ZookeeperRegistryCenter;
import com.asiainfo.dacp.dp.server.scheduler.bean.AgentIps;
import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbSearch;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbStorage;
import com.asiainfo.dacp.scheduler.zk.AgentListenerManager;
import com.asiainfo.dacp.scheduler.zk.ElectionCandidateListener;
import com.asiainfo.dacp.scheduler.zk.RegistryCenterConnectionStateListener;
import com.asiainfo.dacp.scheduler.zk.ServerListenerManager;

/**
 * Zookeeper服务
 * @author zhangqi
 *
 */
@Service
public class ZkService {
	
	private Logger LOG = LoggerFactory.getLogger(ZkService.class);

	@Autowired
	private RegistryCenterConnectionStateListener rccsl;
	@Autowired
	private ElectionCandidateListener ecl;
	@Autowired
	private TaskProcessService tps;
	@Autowired
	private TaskEventRdbStorage storage;
	@Autowired
	private TaskEventRdbSearch search;
	
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
	
	private static final String SEVER_HOST_NODE = "/server/instance";
	private static final String SEVER_RUN_NODE = "server/run";
	private static final String AGENT_HOST_NODE = "/agent/instance";
	private static final String AGENT_RUN_NODE = "/agent/run";
	
	private String SERVER_ID;
	
	private CoordinatorRegistryCenter regCenter;
	
	private ZookeeperConfiguration setUpRegistryCenter(){
		ZookeeperConfiguration zkConfig = new ZookeeperConfiguration(getServerLists(), namespace);
		zkConfig.setSessionTimeoutMilliseconds(sessionTimeoutMilliseconds);
		zkConfig.setConnectionTimeoutMilliseconds(connectionTimeoutMilliseconds);
		if(!StringUtils.isEmpty(digest)){
			zkConfig.setDigest(digest);
		}
		return zkConfig;
	}
	
	private CuratorFramework getClient() {
        return (CuratorFramework) regCenter.getRawClient();
    }
	
	public void init(){
		regCenter = new ZookeeperRegistryCenter(this.setUpRegistryCenter());
		regCenter.init();
		this.registryListener();
		this.fillEphemeralServerRunTime(getSERVER_ID(), "live");	
	}
	
	public void addAgentAndServerListener(){
		new AgentListenerManager(regCenter,tps,storage).start();
		new ServerListenerManager(regCenter,tps,storage,this.getSERVER_ID()).start();
	}
	
	private void registryListener() {
		ZookeeperElectionService service = new ZookeeperElectionService(getSERVER_ID(),
				(CuratorFramework) regCenter.getRawClient(), SEVER_HOST_NODE, ecl);
		service.start();
		this.addConnectionStateListener(rccsl);
	}
    
	private String getServerRunPath(final String severCode) {
        return String.format("/%s/%s", SEVER_RUN_NODE, severCode);
    }
	
	public void initAgentStatus() {
		List<String> liveAgent = regCenter.getChildrenKeys(AGENT_HOST_NODE);
		for(String agentName : liveAgent){
			if(MemCache.AGENT_IPS_MAP.containsKey(agentName)){
				LOG.info("[{}] agent is online",agentName);
        		MemCache.AGENT_IPS_MAP.get(agentName).setAgentStatus(1);
        		//当前并发数不能设置为0,避免和AgentRunTimeListener冲突
        		storage.updateAgentInfo(agentName,"1",null);
			}			
		}
    }
	
	
	/**
	 * 刷新agent配置变化
	 */
	public void refurbishAgentConfig() {
		List<AgentIps> ipsList = search.queryAgentIps();
		List<String> temp_ = new ArrayList<String>();
		for (AgentIps ips : ipsList) {
			temp_.add(ips.getAgentCode());
			//新agent
			if(!MemCache.AGENT_IPS_MAP.containsKey(ips.getAgentCode())){
				ips.setAgentStatus(regCenter.isExisted(AGENT_HOST_NODE + "/" + ips.getAgentCode()) ? 1:0);
				ips.setCurips(regCenter.getNumChildren(AGENT_RUN_NODE + "/" + ips.getAgentCode()));
				MemCache.AGENT_IPS_MAP.put(ips.getAgentCode(), ips);
				storage.updateAgentInfo(ips.getAgentCode(),String.valueOf(ips.getAgentStatus()),String.valueOf(ips.getCurips()));
				LOG.info("new agent [{}] ,it's status:{}",ips.getAgentCode(),ips.getAgentStatus());
			}else{
				MemCache.AGENT_IPS_MAP.get(ips.getAgentCode()).setIps(ips.getIps());
				MemCache.AGENT_IPS_MAP.get(ips.getAgentCode()).setPlatform(ips.getPlatform());;
			}
		}
		
		//被删除的agent
		Set<Map.Entry<String, AgentIps>> set = MemCache.AGENT_IPS_MAP.entrySet();
		Iterator<Map.Entry<String, AgentIps>> it = set.iterator();
		while (it.hasNext()) {
			Map.Entry<String, AgentIps> item = it.next();
			if(!temp_.contains(item.getKey())){
				MemCache.AGENT_IPS_MAP.remove(item.getKey());
				LOG.info("agent [{}] was delete",item.getKey());
			}
		}
	}
	
	/**
	 * 获取agent并发数(瞬时并发控制)
	 * @param agentCode
	 * @return
	 */
	public int getAgentCurips(String agentCode){
		return regCenter.getNumChildren(AGENT_RUN_NODE + "/" + agentCode);
	}
	
	/**
     * 注册连接状态监听器.
     * 
     * @param listener 连接状态监听器
     */
    public void addConnectionStateListener(final ConnectionStateListener listener) {
        getClient().getConnectionStateListenable().addListener(listener);
    }
    
    public void fillEphemeralServerRunTime(final String serverCode, final Object value) {
        regCenter.persistEphemeral(this.getServerRunPath(serverCode), value.toString());
    }
    
    /**
     * 判断Server进程节点是否存在.
     * 
     */
    public boolean isServerRunTimeExisted(final String serverCode) {
        return regCenter.isExisted(this.getServerRunPath(serverCode));
    }
    
    /**
     * 删除Server进程节点.
     * 
     */
    public void removeServerRunTimeIfExisted(final String serverCode) {
        if (isServerRunTimeExisted(serverCode)) {
            regCenter.remove(this.getServerRunPath(serverCode));
        }
    }

	public String getServerLists() {
		return serverLists;
	}

	public void setServerLists(String serverLists) {
		this.serverLists = serverLists;
	}

	public String getSERVER_ID() {
		return SERVER_ID;
	}

	public void setSERVER_ID(String sERVER_ID) {
		SERVER_ID = sERVER_ID;
	}
}
