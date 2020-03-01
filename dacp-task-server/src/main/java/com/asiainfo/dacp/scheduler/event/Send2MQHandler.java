package com.asiainfo.dacp.scheduler.event;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.asiainfo.dacp.dp.common.RunStatus;
import com.asiainfo.dacp.dp.server.scheduler.bean.AgentIps;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskConfig;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskLog;
import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbStorage;
import com.asiainfo.dacp.dp.server.scheduler.type.Type;
import com.asiainfo.dacp.dp.tools.TimeUtils;
import com.asiainfo.dacp.scheduler.service.TaskCacheService;
import com.asiainfo.dacp.scheduler.service.TaskService;
import com.asiainfo.dacp.scheduler.service.ZkService;
import com.lmax.disruptor.EventHandler;

/**
 * 发送Agent
 * @author zhangqi
 *
 */
@Service
public class Send2MQHandler implements EventHandler<TaskLogEvent> {
	
	@Autowired
	private TaskEventRdbStorage storage;		
	@Autowired
	private TaskService ts;
	@Autowired
	private ZkService zks;
	
	private Logger LOG = LoggerFactory.getLogger(Send2MQHandler.class);
	
	@Override
	public void onEvent(TaskLogEvent event, long sequence, boolean endOfBatch) throws Exception {
		
		try {
			TaskLog tl = event.getTl();
			String agentCode = tl.getAgentCode();
			String platform = tl.getPlatform();
			
			//暂停任务的判断
			if(MemCache.PROC_PAUSE_MAP.containsKey(tl.getSeqno())){
				MemCache.TASK_WAIT_REASON.put(tl.getSeqno(), RunStatus.ERRCODE.PAUSE);
				return;
			}
			
			//规避空格等脏数据
			agentCode = (agentCode != null) ? agentCode.trim() : agentCode;
			platform = (platform != null) ? platform.trim() : platform;
			
			if (StringUtils.isEmpty(platform)) {
				TaskConfig config = MemCache.PROC_MAP.get(tl.getXmlid());
				platform = config != null ? config.getPlatform() : null;
			}
			//都为空的情况,直接失败
			if (StringUtils.isEmpty(agentCode) && StringUtils.isEmpty(platform)){
				String errorInfo = String.format("task[%s] check agent-ips error[configure error] : both the configure of the agent and platform is null", tl.getTaskId());
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("seqno", tl.getSeqno());
				map.put("generate_time", TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"));
				map.put("app_log", errorInfo);
				LOG.error(errorInfo);			
				storage.setTaskError2QueueOut(tl.getSeqno());
				storage.insert2Transaction("proc_schedule_script_log", map);
				return;
			}
			
			//筛选agent
			boolean hasPlatform = false;
			if (StringUtils.isEmpty(agentCode)) {
				int ipsOffset = 0, maxOffset = 0;
				for (AgentIps agent : MemCache.AGENT_IPS_MAP.values()) {
					if (agent.getAgentStatus() == 1
							&& StringUtils.equalsIgnoreCase(agent.getPlatform(), tl.getPlatform())) {
						hasPlatform = true;
						ipsOffset = agent.getIps() - agent.getCurips();
						// 取最空闲的agent来执行
						if (ipsOffset > maxOffset) {
							maxOffset = ipsOffset;
							agentCode = agent.getAgentCode();
						}
					}
				}
			}
			
			if(StringUtils.isEmpty(agentCode)){
				int status = RunStatus.AGENT.EXCEPTION;
//				if(hasPlatform){
//					status = RunStatus.AGENT.FULL; 
//				}else{
//					status = RunStatus.AGENT.NEED;
//				}
				MemCache.TASK_WAIT_REASON.put(tl.getSeqno(), status);
				return;
			}
			
			AgentIps _agent = MemCache.AGENT_IPS_MAP.get(agentCode);
			if (_agent == null) {
				String errorInfo = String.format("task[%s] check agent-ips error:no agent[%s] ", tl.getTaskId(),agentCode);
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("seqno", tl.getSeqno());
				map.put("generate_time", TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"));
				map.put("app_log", errorInfo);
				LOG.error(errorInfo);			
				storage.setTaskError2QueueOut(tl.getSeqno());
				storage.insert2Transaction("proc_schedule_script_log", map);
				return;
			}
			if (_agent.getAgentStatus() == 0) {
				MemCache.TASK_WAIT_REASON.put(tl.getSeqno(), RunStatus.AGENT.DEAD);
				return;
			}
			
			if (_agent.getCurips() >= _agent.getIps()) {
				LOG.debug("zk缓存agent {} 已满，任务{}进入排队。当前agent{}/{}状态",_agent.getAgentCode(),tl.getSeqno(),_agent.getCurips(),_agent.getIps());
				MemCache.TASK_WAIT_REASON.put(tl.getSeqno(), RunStatus.AGENT.FULL);
				return;
			}
			
			//实时取zk里面任务并发数
			int curips = zks.getAgentCurips(_agent.getAgentCode());
			if(curips >= _agent.getIps()){
				MemCache.TASK_WAIT_REASON.put(tl.getSeqno(), RunStatus.AGENT.FULL);
				LOG.debug("zk实时检测agent {} 已满，任务{}进入排队。当前agent{}/{}状态",_agent.getAgentCode(),tl.getSeqno(),curips,_agent.getIps());
				return;
			}
			
			//实时取缓存里面任务并发
			int agentips = TaskCacheService.getAgentRunning(agentCode).size();
			if(agentips >= _agent.getIps()){
				MemCache.TASK_WAIT_REASON.put(tl.getSeqno(), RunStatus.AGENT.FULL);
				LOG.info("缓存实时检测agent {} 已满，任务{}进入排队。当前agent{}/{}状态",_agent.getAgentCode(),tl.getSeqno(),agentips,_agent.getIps());
				return;
			}
			
//			int i = 0;
//			if(MemCache.AGENT_IPS_MAP_MC.containsKey(agentCode)){
//				i = MemCache.AGENT_IPS_MAP_MC.get(agentCode);
//				MemCache.AGENT_IPS_MAP_MC.put(agentCode, ++i);
//			}else{
//				MemCache.AGENT_IPS_MAP_MC.put(agentCode, ++i);
//			}
			
//			if(MemCache.AGENT_IPS_MAP_MC.get(agentCode) >= _agent.getIps()){
//				MemCache.TASK_WAIT_REASON.put(tl.getSeqno(), RunStatus.AGENT.FULL);
//				return;
//			}
			
			tl.setAgentCode(agentCode);
//			if(this.checkRunModelAgain(MemCache.PROC_MAP.get(tl.getXmlid()))){
//				MemCache.TASK_WAIT_REASON.put(tl.getSeqno(), RunStatus.ERRCODE.MUT);
//				return;
//			}
			// 发送至agent
			TaskCacheService.addAgentRunning(agentCode,tl.getSeqno());
			if (!ts.sendProcToAgent(tl)) {
				TaskCacheService.removeAgentRunning(agentCode,tl.getSeqno());
				MemCache.TASK_WAIT_REASON.put(tl.getSeqno(), RunStatus.AGENT.FAIL);
				return;
			}
			//清除MAP
			MemCache.TASK_WAIT_REASON.remove(tl.getSeqno());
			storage.updateTaskState2AgentCode(tl.getSeqno(), RunStatus.SEND_TO_MQ,agentCode);
		} catch (Exception e) {
			LOG.error("hard error,{}",event.getTl()== null ? "can't found taskLog" : event.getTl().getTaskId());
			e.printStackTrace();
		}
	}
	
	private boolean checkRunModelAgain(TaskConfig tc){
		if(MemCache.RUN_TASK.containsKey(tc.getXmlid())){
			int mutiRun = tc.getMutiRunFlag();
			if(mutiRun == Type.RUN_TYPE.EXECUTE_SERIAL.ordinal() || mutiRun == Type.RUN_TYPE.EXECUTE_ONECE.ordinal()){
				return true;
			}
		}
		return false;
	}
}