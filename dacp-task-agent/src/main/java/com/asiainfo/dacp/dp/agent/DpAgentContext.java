package com.asiainfo.dacp.dp.agent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.asiainfo.dacp.dp.message.DpReceiver;
import com.asiainfo.dacp.dp.message.DpSender;
import com.asiainfo.dacp.process.DpProcess;
import com.asiainfo.dacp.dp.message.DpMessage;
import com.asiainfo.dacp.zk.AgentZkOperation;

/**
 * 
 * @author MeiKefu
 * @date 2014-12-18
 */
@Component
public class DpAgentContext {
	/**代理服务器名*/
	@Value("${agent-code}")
	private String agentCode;
	/**请求队列名后缀*/
	@Value("${request.queue.name}")
	private String agentQueueSuffix;
	/**代理服务器名*/
	@Value("${log-path}")
	private String logPath;
	/**task-server接收消息默认队列*/
	@Value("${task-server-queue}")
	private String taskServerQueue;
	/**要执行的shell脚本路径*/
	@Value("${kill-shell}")
	private String shellPath;
	@Value("${runlog2db}")
	private boolean runLog2db;
	
	public String getShellPath() {
		return shellPath;
	}
	/**消息处理发送器*/
	@Autowired
	private DpSender dpSender;
	/**进程管理工具*/
	@Autowired
	private DpProcess dpProcess;
	@Autowired
	private AgentZkOperation azko;
	@Autowired
	private DpReceiver dpReceiver;
	private ApplicationContext appContext;
	/**进程队列*/
	private Map<String,Process> processMap = new ConcurrentHashMap<String, Process>();
	/**进程日志获取队列*/
	private Map<String,FutureTask<Boolean>> futureTaskMap = new ConcurrentHashMap<String, FutureTask<Boolean>>();
	
	private ExecutorService threadPool = Executors.newCachedThreadPool();
	
	public ExecutorService getThreadPool() {
		return threadPool;
	}
	public String getAgentCode() {
		return agentCode;
	}
	public DpProcess getDpProcess() {
		return dpProcess;
	}
	
	public DpSender getDpSender() {
		return dpSender;
	}
	
	public String getLogPath() {
		return logPath;
	}
	public Map<String, Process> getProcessMap() {
		return processMap;
	}
	
	public void offerSendQueue(DpMessage msg){
		dpSender.sendMessage(msg.getSourceQueue(), msg, -1);
	}
	public ApplicationContext getAppContext() {
		return appContext;
	}
	public void setAppContext(ApplicationContext appContext) {
		this.appContext = appContext;
	}
	public DpReceiver getDpReceiver() {
		return dpReceiver;
	}

	public AgentZkOperation getAzko() {
		return azko;
	}
	public Map<String,FutureTask<Boolean>> getFutureTaskMap() {
		return futureTaskMap;
	}
	public boolean isRunLog2db() {
		return runLog2db;
	}
	public void setRunLog2db(boolean runLog2db) {
		this.runLog2db = runLog2db;
	}
	
}
