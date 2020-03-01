package com.asiainfo.dacp.scheduler.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.asiainfo.dacp.dp.common.RunStatus;
import com.asiainfo.dacp.dp.server.scheduler.bean.AgentIps;
import com.asiainfo.dacp.dp.server.scheduler.bean.SourceObj;
import com.asiainfo.dacp.dp.server.scheduler.bean.TargetObj;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskConfig;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskLog;
import com.asiainfo.dacp.dp.server.scheduler.bean.TransdatamapDesignBean;
import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbSearch;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbStorage;
import com.asiainfo.dacp.dp.server.scheduler.type.DBType;
import com.asiainfo.dacp.dp.server.scheduler.type.ObjType;
import com.asiainfo.dacp.dp.server.scheduler.type.RunFreq;
import com.asiainfo.dacp.dp.tools.TimeUtils;

/**
 * 数据库刷新到缓存
 * @author zhangqi
 *
 */
@Service
public class TaskCacheService {
	
	private static Logger LOG = LoggerFactory.getLogger(TaskCacheService.class);
	
	@Autowired
	private TaskEventRdbSearch search;
	@Autowired
	private TaskEventRdbStorage storage;
	@Autowired
	private BasicDataSource basicDataSource;
	@Autowired
	private TaskDisruptorService tds;
	@Autowired
	private TaskService ts;
	
	@Value("${rule.run.afterToday}")
	private boolean isAfterToday;
	public CountDownLatch latch;

	/** 加载缓存数据 
	 * @throws InterruptedException */
	public void initCache() throws InterruptedException{
		this.distinguishDBType();
		this.initTaskConfig();
		this.refurbishTransdatamapDesign(null);
	}
	
	private void distinguishDBType() {
		String driverClass = basicDataSource.getDriverClassName();
		if ("com.mysql.jdbc.Driver".equals(driverClass)) {
			MemCache.DBTYPE = DBType.MYSQL;
		} else if ("oracle.jdbc.driver.OracleDriver".equals(driverClass)) {
			MemCache.DBTYPE = DBType.ORACLE;
		}
	}
	
	/**
	 * 刷新任务配置
	 * @param xmlid
	 */
	public void refurbishTransdatamapDesign(String xmlid){
		List<TransdatamapDesignBean> tdbs = search.queryTransdatamapDesign(xmlid);
		LOG.info("刷新任务关系配置:{}",tdbs.size());
		for(TransdatamapDesignBean tdb : tdbs){
			if(MemCache.TARGET_MAP.containsKey(tdb.getSource())){
				//触发目标为有效程序,或者是数据
				if(MemCache.PROC_MAP.containsKey(tdb.getTarget()) || !ObjType.PROC.name().equals(tdb.getTargettype())){
					MemCache.TARGET_MAP.get(tdb.getSource()).put(tdb.getTarget(), this.getTargetObj(tdb));
				}
			}else{
				//触发源为有效程序,或者是数据
				if(MemCache.PROC_MAP.containsKey(tdb.getSource()) || !ObjType.PROC.name().equals(tdb.getSourcetype())){
					//触发目标为有效程序,或者是数据
					if(MemCache.PROC_MAP.containsKey(tdb.getTarget()) || !ObjType.PROC.name().equals(tdb.getTargettype())){
						ConcurrentHashMap<String,TargetObj> chm_t = new ConcurrentHashMap<String,TargetObj>();
						chm_t.put(tdb.getTarget(), this.getTargetObj(tdb));
						MemCache.TARGET_MAP.put(tdb.getSource(), chm_t);
					}					
				}				
			}
			if(MemCache.SOURCE_MAP.containsKey(tdb.getTarget())){
				//依赖程序属于有效 或者依赖类型是数据
				if(MemCache.PROC_MAP.containsKey(tdb.getSource()) || !ObjType.PROC.name().equals(tdb.getSourcetype())){
					MemCache.SOURCE_MAP.get(tdb.getTarget()).put(tdb.getSource(), this.getSourceObj(tdb));
				}
			}else{
				//依赖关系目标程序并且属于有效,数据依赖无意义,体现在程序的触发列表
				if(MemCache.PROC_MAP.containsKey(tdb.getTarget()) || !ObjType.PROC.name().equals(tdb.getTargettype())){
					//依赖程序属于有效 或者依赖类型是数据
					if(MemCache.PROC_MAP.containsKey(tdb.getSource()) || !ObjType.PROC.name().equals(tdb.getSourcetype())){
						ConcurrentHashMap<String,SourceObj> chm_s = new ConcurrentHashMap<String,SourceObj>();
						chm_s.put(tdb.getSource(), this.getSourceObj(tdb));
						MemCache.SOURCE_MAP.put(tdb.getTarget(), chm_s);
					}
				}				
			}
		}
	}
	
	private SourceObj getSourceObj(TransdatamapDesignBean tdb){
		SourceObj to = new SourceObj();
		to.setTarget(tdb.getTarget());
		to.setSource(tdb.getSource());
		to.setSourcetype(tdb.getSourcetype());
		to.setSourcefreq(tdb.getSourcefreq());
		return to;
	}
	
	private TargetObj getTargetObj(TransdatamapDesignBean tdb){
		TargetObj to = new TargetObj();
		to.setSource(tdb.getSource());
		to.setSourcefreq(tdb.getSourcefreq());
		to.setSourcetype(tdb.getSourcetype());
		to.setTarget(tdb.getTarget());
		to.setTargetfreq(tdb.getTargetfreq());
		to.setTargettype(tdb.getTargettype());
		return to;
	}
	
	/***
	 * 加载配置
	 */
	public void initTaskConfig() {
		List<TaskConfig> procList = search.queryTaskConfigList();
		LOG.info("初始化任务配置:{}",procList.size());
		//初始化任务配置信息
		for (TaskConfig config : procList) {
			MemCache.PROC_MAP.put(config.getXmlid(), config);
		}
	}
	
	public void initAgentConfig() {
		List<AgentIps> ipsList = search.queryAgentIps();
		for (AgentIps ips : ipsList) {
			ips.setAgentStatus(0);//初始化默认为agent offline
			ips.setCurips(0);//初始化默认为0
			MemCache.AGENT_IPS_MAP.put(ips.getAgentCode(), ips);
			storage.updateAgentInfo(ips.getAgentCode(),"0",null);//初始都设置为失效
		}
	}
	
	public void validCache() {
		//agent并发
		for (String agentCode : MemCache.AGENT_RUN_MAP.keySet()) {
	         HashSet<String> running = MemCache.AGENT_RUN_MAP.get(agentCode);
	         for(String seqno : running) {
	        	 TaskLog tasklog = search.queryRunningTaskRunLog(seqno);
	        	 if(tasklog==null) {
	        		 removeAgentRunning(agentCode,seqno);
	        	 }
	         }
	    }
		
		//任务运行
		for (String xmlid : MemCache.RUN_TASK.keySet()) {
	         String runningjob = MemCache.RUN_TASK.get(xmlid);
	         if(runningjob!=null||!"".equals(runningjob)) {
	        	 String[] batchs = runningjob.split(",");
	        	 for(String batch : batchs) {
	        		 TaskLog tasklog = search.queryRunningTaskRunLog(xmlid,batch);
	        		 if(tasklog==null) {
	        			 removeRunTaskTemp(xmlid,batch);
	        		 }
	        	 }
	         }
	    }
		
		//任务配置信息
		List<TaskConfig> procList = search.queryTaskConfigList();
		for (String key : MemCache.PROC_MAP.keySet()) {
			TaskConfig tsc = MemCache.PROC_MAP.get(key);
			boolean isExists = false;
	        for(TaskConfig tc : procList) {
	        	if(tc.getXmlid().equals(tsc.getXmlid())) {
	        		isExists=true;
	        		break;
	        	}
	        }
	        if(!isExists) {
	        	try {
	        		ts.clearTaskCfgInTransdatamapDesign(tsc.getXmlid());
					ts.refurbishProcConfig2Dead(tsc);
					LOG.info("update config of task[{}] success,to dead",tsc.getProcName());
				} catch (Exception e) {
					LOG.error("task[{}] 下线失败",tsc.getProcName());
				}
				
	        }
	    }
	}
	
	public void refurbishTaskRunLog() throws Exception{		
		long start = System.currentTimeMillis();
		List<TaskLog> taskRunLogList = search.queryTaskRunLogList();
		latch = new CountDownLatch(taskRunLogList.size());
		LOG.info("读取待处理任务:{},time taken:{},latch_size {}",taskRunLogList.size(),TimeUtils.formatLong(System.currentTimeMillis()-start),latch.getCount());
		//清楚运行态缓存
//		MemCache.RUN_TASK.clear();
		MemCache.AGENT_IPS_MAP_MC.clear();
		List<TaskLog> stm = new ArrayList<TaskLog>();	
		for (TaskLog taskLog : taskRunLogList) {
			if (taskLog.getRunFreq().equals(RunFreq.manual.name())) {
				//TODO 后期补充
				tds.putData(taskLog,tds.send2MQRB,RunStatus.CHECK_RUNMODEL_SUCCESS);
				continue;
			}
			if(!MemCache.PROC_MAP.containsKey(taskLog.getXmlid())){
				LOG.error("任务[{}]调度配置已失效,即将出队相关运行日志",taskLog.getTaskId());
				storage.queueOutTaskLog2Transaction(taskLog.getSeqno());
				latch.countDown();
				continue;
			}
			if(!isAfterToday && !TimeUtils.isBefore2Tomorrow(taskLog.getDateArgs())){
				latch.countDown();
				continue;
			}
			switch (taskLog.getTaskState()) {
			case RunStatus.CREATE_TASK:
				tds.putData(taskLog, tds.checkDpendRB,RunStatus.CREATE_TASK);
				break;
			case RunStatus.CHECK_DEPEND_SUCCESS:
				tds.putData(taskLog,tds.checkRunModelRB,RunStatus.CHECK_DEPEND_SUCCESS);
				break;
			case RunStatus.CHECK_RUNMODEL_SUCCESS:
				stm.add(taskLog);
				break;
			case RunStatus.PROC_RUN_SUCCESS:
				removeRunTaskTemp(taskLog.getXmlid(),taskLog.getDateArgs());
				removeAgentRunning(taskLog.getAgentCode(),taskLog.getSeqno());
				tds.putData(taskLog,tds.sucTriggerRB,RunStatus.PROC_RUN_SUCCESS);
				break;
			case RunStatus.PROC_RUN_FAIL:
				removeRunTaskTemp(taskLog.getXmlid(),taskLog.getDateArgs());
				removeAgentRunning(taskLog.getAgentCode(),taskLog.getSeqno());
				tds.putData(taskLog,tds.failRB,RunStatus.PROC_RUN_FAIL);
				break;
			case RunStatus.SEND_TO_MQ:
				tds.putData(taskLog,tds.mQStuckRB,RunStatus.SEND_TO_MQ);
				break;
			case RunStatus.PROC_RUNNING:
				tds.putData(taskLog,tds.runTimeOutRB,RunStatus.PROC_RUNNING);
				break;
			default:
				latch.countDown();
				break;
			}
		}
		
		if (!stm.isEmpty()) {
			Collections.sort(stm, new Comparator<TaskLog>() {
				@Override
				public int compare(TaskLog o1, TaskLog o2) {
					return (o2.getPriLevel() == null ? 0 : o2.getPriLevel()) - (o1.getPriLevel() == null ? 0 : o1.getPriLevel());
				}
			});
			for(TaskLog taskLog : stm){
				tds.putData(taskLog,tds.send2MQRB,RunStatus.CHECK_RUNMODEL_SUCCESS);
	        }
		}
		
		latch.await();
	}	
	
	public static void addRunTaskTemp(String xmlid,String dateArgs) {
		// 任务运行详情
		if(MemCache.RUN_TASK.get(xmlid)==null||"".equals(MemCache.RUN_TASK.get(xmlid))) {
			MemCache.RUN_TASK.put(xmlid,dateArgs);
		} else {
			String val = MemCache.RUN_TASK.get(xmlid).replaceAll(","+dateArgs, "").replaceAll(dateArgs, "");
			if("".equals(val)) {
				MemCache.RUN_TASK.put(xmlid,dateArgs);
			} else {
				MemCache.RUN_TASK.put(xmlid,val+","+dateArgs);
			}
		}
		
		// 任务agent运行详情
		LOG.debug("add task_run {},{},now running {}",xmlid,dateArgs,MemCache.RUN_TASK.get(xmlid));
	}
	
	public static void removeRunTaskTemp(String xmlid,String dateArgs) {
		if(MemCache.RUN_TASK.get(xmlid)==null||"".equals(MemCache.RUN_TASK.get(xmlid))) {
			MemCache.RUN_TASK.remove(xmlid);
		} else {
			String val = MemCache.RUN_TASK.get(xmlid).replaceAll(","+dateArgs, "").replaceAll(dateArgs, "");
			if("".equals(val)) {
				MemCache.RUN_TASK.remove(xmlid);
			} else {
				MemCache.RUN_TASK.put(xmlid,val);
			}
		}
		LOG.debug("remove task_run {},{},now running {}",xmlid,dateArgs,MemCache.RUN_TASK.get(xmlid));
	}
	
	public static int addAgentRunning(String agentCode,String seqno) {
		HashSet<String> running = MemCache.AGENT_RUN_MAP.get(agentCode);
		if(running==null) {
			running=new HashSet<String>();
		}
		running.add(seqno);
		MemCache.AGENT_RUN_MAP.put(agentCode, running);
		return running.size();
	}
	
	public static int removeAgentRunning(String agentCode,String seqno) {
		if(agentCode==null||"".equals(agentCode)) {
			return 0;
		}
		HashSet<String> running = MemCache.AGENT_RUN_MAP.get(agentCode);
		if(running==null) {
			return 0;
		}
		running.remove(seqno);
		MemCache.AGENT_RUN_MAP.put(agentCode, running);
		return running.size();
	}
	
	public static HashSet<String> getAgentRunning(String agentCode) {
		if(agentCode==null||"".equals(agentCode)) {
			return new HashSet<String>();
		}
		HashSet<String> running = MemCache.AGENT_RUN_MAP.get(agentCode);
		if(running==null) {
			return new HashSet<String>();
		}
		return running;
	}
}
