package com.asiainfo.dacp.scheduler.service;

import java.text.ParseException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.StringUtils;
import org.quartz.Job;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.ai.zq.common.util.concurrent.BlockUtils;
import com.ai.zq.common.util.env.IpUtils;
import com.asiainfo.dacp.dp.message.DpReceiver;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskConfig;
import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbStorage;
import com.asiainfo.dacp.dp.server.scheduler.type.RunFreq;
import com.asiainfo.dacp.dp.server.scheduler.type.Type;
import com.asiainfo.dacp.dp.tools.FIFOLinkedHashMap;
import com.asiainfo.dacp.dp.tools.ReflectSysClassLoader;
import com.asiainfo.dacp.dp.tools.TimeUtils;
import com.asiainfo.dacp.scheduler.quartz.CheckOnOffJob;
import com.asiainfo.dacp.scheduler.quartz.ClearMinTaskJob;
import com.asiainfo.dacp.scheduler.quartz.ClearTranshTaskJob;
import com.asiainfo.dacp.scheduler.quartz.CreatePlanJob;
import com.asiainfo.dacp.scheduler.quartz.JobScheduleController;
import com.asiainfo.dacp.scheduler.quartz.QuartzJob;
import com.asiainfo.dacp.scheduler.quartz.RefurbishAgentCfgJob;
import com.asiainfo.dacp.scheduler.quartz.RefurbishTableInfoJob;
import com.asiainfo.dacp.scheduler.quartz.TaskAnalysisJob;
import com.asiainfo.dacp.scheduler.quartz.TaskQuartzFactory;
import com.asiainfo.dacp.scheduler.runnable.DbExecute2TranThread;
import com.asiainfo.dacp.scheduler.runnable.MonitorDbExecute2TranThread;

/**
 * 主控逻辑
 * @author zhangqi
 *
 */
@Service
public class TaskProcessService {
	
	private static Logger LOG = LoggerFactory.getLogger(TaskProcessService.class);
	@Value("${monitor.dbtran.start}")
	private boolean isMonitor;
	@Value("${jetty.start}")
	private boolean isJettyStart;
	
	@Value("${service.analysis.runinfo}")
	private boolean isAnalysisRunInfo;
	@Value("${service.analysis.cronExp}")
	private String analysisRunInfoCronExp;
	
	@Value("${service.clear.minTask}")
	private boolean isClearMinTask;
	@Value("${service.clear.minTask.cronExp}")
	private String clearMinTaskCronExp;
	
	@Value("${service.clear.trashTask}")
	private boolean isClearTrashTask;
	@Value("${service.clear.trashTask.cronExp}")
	private String clearTrashTaskCronExp;
	
	@Value("${service.plan.create}")
	private boolean isPlanCreate;
	@Value("${service.plan.create.cronExp}")
	private String planCreateCronExp;
	
	@Value("${service.alarm}")
	private boolean isAlarm;
	@Value("${service.alarm.cronExp}")
	private String alarmCronExp;
	@Value("${service.alarm.class}")
	private String alarmClass;
	
//	@Value("${suc.bufferSize}")
//	private int maxCapacity;
	
	@Autowired
	private TaskCacheService tcs;
	@Autowired
	private TaskService ts;
	@Autowired
	private TaskDisruptorService tds;
	@Autowired
	private ZkService zks;
	@Autowired
	private JettyService jettyService;
	@Autowired
	private DbExecute2TranThread stdbthread;
	@Autowired
	private MonitorDbExecute2TranThread mstdbthread;
	@Autowired
	private TaskEventRdbStorage storage;
	@Autowired
	private TaskQuartzFactory  quartzFactory;
	@Autowired
	private JobScheduleController jobScheduleController;
	@Autowired
	public DpReceiver dpReceiver;
	
	private boolean isZkLeader;
	private boolean isZkEnvNormal;
	private ExecutorService threads = Executors.newCachedThreadPool();

	@Value("${jetty.port}")
	private int jettyPort;
	
	private void init() throws InterruptedException, SchedulerException, ClassNotFoundException{
		threads.execute(stdbthread);
		if(isMonitor){
			threads.execute(mstdbthread);
		}
		quartzFactory.createScheduler();
		jobScheduleController.setScheduler(quartzFactory.getScheduler());
		if(isAnalysisRunInfo){
			LOG.info("AnalysisRunInfo service CronExp [{}] is activated",analysisRunInfoCronExp);
			jobScheduleController.scheduleJob("AnalysisRunInfo", analysisRunInfoCronExp, TaskAnalysisJob.class);
		}
		if(isClearMinTask){
			LOG.info("ClearMinTask service CronExp [{}] is activated",clearMinTaskCronExp);
			jobScheduleController.scheduleJob("ClearMinTask", clearMinTaskCronExp, ClearMinTaskJob.class);
		}
		if(isClearTrashTask){
			LOG.info("ClearTrashTask service CronExp [{}] is activated",clearTrashTaskCronExp);
			jobScheduleController.scheduleJob("ClearTrashTask", clearTrashTaskCronExp, ClearTranshTaskJob.class);
		}
		if(isPlanCreate){
			LOG.info("PlanCreate service CronExp [{}] is activated",planCreateCronExp);
			jobScheduleController.scheduleJob("PlanCreate", planCreateCronExp, CreatePlanJob.class);
		}
		if(isAlarm){
			LOG.info("Alarm service[{}] alarmCronExp [{}] is activated",alarmClass,alarmCronExp);
			jobScheduleController.scheduleJob("Alarm", alarmCronExp, (Class<? extends Job>) ReflectSysClassLoader.sysloader.loadClass(alarmClass));
		}
		
		//系统内部job不提供个性化配置
		LOG.info("Refurbish AgentCfg service is activated");
		jobScheduleController.scheduleJob("RefurbishAgentCfg", "0 0/2 * * * ?", RefurbishAgentCfgJob.class);
		
		//系统内部job不提供个性化配置
		LOG.info("Refurbish TableInfo service is activated");
		jobScheduleController.scheduleJob("RefurbishTableInfo", "0 0/5 * * * ?", RefurbishTableInfoJob.class);
		
		// 系统内部job不提供个性化配置
		LOG.info("CheckOnOFF service is activated");
		jobScheduleController.scheduleJob("CheckOnOFF", "0 0/3 * * * ?", CheckOnOffJob.class);

		tds.init();
		//MemCache.EVENT_SUC_FIFO = new FIFOLinkedHashMap<String, Integer>(maxCapacity);
		tcs.initCache();		
		this.timeTrigger();
		dpReceiver.start();
	}
	
	public void doit(String serverId,long debugSleep,String ip) throws NumberFormatException, ParseException, Exception{
		zks.setSERVER_ID(serverId.concat("@_@").concat(StringUtils.isEmpty(ip) ? IpUtils.getIp() : ip));
		LOG.info("Server [{}] is ready for start",zks.getSERVER_ID());
		isZkEnvNormal = true;				
		storage.addServerStatus(zks.getSERVER_ID(), 0, jettyPort);
		zks.init();
		while(!isZkLeader){
			BlockUtils.sleep(2000l);
		}
		tcs.initAgentConfig();
		zks.initAgentStatus();
		zks.addAgentAndServerListener();
		storage.addServerStatus(zks.getSERVER_ID(), 1, jettyPort);
		if(isJettyStart){
			jettyService.startJettyServer(ResultController.class);
		}
		this.init();
		while(true){
			if(!isZkEnvNormal || !isZkLeader){
				BlockUtils.waitingShortTime();
				continue;
			}
			try {
				
				long t1 = System.currentTimeMillis();
				ts.refurbishProcConfig();
				long t2 = System.currentTimeMillis();
				showConsumeTime("refurbish ProcConfig", t1, t2);
				ts.redoTaskFollowUpOptimize();
				long t3 = System.currentTimeMillis();
				showConsumeTime("redoTaskFollowUp Optimize", t2, t3);
				ts.outIntfDataTrigger();
				long t4 = System.currentTimeMillis();
				showConsumeTime("outIntfDataTrigger", t3, t4);
				tcs.refurbishTaskRunLog();
				long t5 = System.currentTimeMillis();
				showConsumeTime("refurbishTaskRunLog", t4, t5);
				tcs.validCache();
				long t6 = System.currentTimeMillis();
				showConsumeTime("validRunningCache", t5, t6);
				
				//测试用
				if (debugSleep > 0) {
					BlockUtils.sleep(debugSleep);
				}
			} catch (Exception e) {
				LOG.error("Oops!, this loop has an exception.", e);
			}
		}
	}
	
	/**
	 * 打印消耗时间
	 * @param msg 提示信息
	 * @param start 开始时间
	 * @param end 结束时间
	 * @param interval 消耗时间超过此阀值，打印信息，单位秒
	 */
	private void showConsumeTime(String msg,long start,long end,int interval){
		long diff = (end - start) / 1000;
		if(diff > interval){
			LOG.info("{} processing speed is so slow,{}",msg,TimeUtils.formatLong(end-start));
		}
	}
	
	/**
	 * 打印消耗时间，默认超过30秒打印
	 * @param msg 提示信息
	 * @param start 开始时间
	 * @param end 结束时间
	 */
	private void showConsumeTime(String msg,long start,long end){
		showConsumeTime( msg, start, end, 20);
	}
	
	/***
	 * 时间触发
	 * @throws SchedulerException 
	 */
	private void timeTrigger() throws SchedulerException {
		for (TaskConfig config : MemCache.PROC_MAP.values()) {
			if (config.getTriggerType().intValue() == Type.DRIVE_TYPE.TIME_TRIGGER.ordinal()) {
				if (StringUtils.equals(config.getRunFreq(), RunFreq.manual.name())) {
					continue;
				}
				try {
					jobScheduleController.scheduleJob(config.getXmlid(), config.getCronExp(), QuartzJob.class);
					LOG.info("create schdule job [{}.{},{}] is success", new String[]{config.getXmlid(),config.getProcName(),config.getCronExp()});
				} catch (Exception e) {
					LOG.error("create schdule job [{}.{},{}] is fail", new String[]{config.getXmlid(),config.getProcName(),config.getCronExp()},e);
				}				
			}
		}		
	}

	public boolean isZkLeader() {
		return isZkLeader;
	}

	public void setZkLeader(boolean isZkLeader) {
		this.isZkLeader = isZkLeader;
	}

	public boolean isZkEnvNormal() {
		return isZkEnvNormal;
	}

	public void setZkEnvNormal(boolean isZkEnvNormal) {
		this.isZkEnvNormal = isZkEnvNormal;
	}

}
