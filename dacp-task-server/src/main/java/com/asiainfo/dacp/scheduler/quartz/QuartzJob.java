package com.asiainfo.dacp.scheduler.quartz;

import java.util.Date;

import org.quartz.CronExpression;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.asiainfo.dacp.dp.server.scheduler.bean.TaskConfig;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskLog;
import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.scheduler.event.CheckRunModelHandler;
import com.asiainfo.dacp.scheduler.service.TaskService;


/**
 * 时间触发任务
 * TODO 需要优化,提示分钟级别任务效率
 * @author zhangqi
 *
 */
public class QuartzJob implements Job {
	@Autowired
	private TaskService ts;
	
	private Logger LOG = LoggerFactory.getLogger(QuartzJob.class);

	public void execute(JobExecutionContext context) throws JobExecutionException {
		String xmlid = context.getJobDetail().getKey().getName();
		LOG.info("job trriger xmlid {}",context.getJobDetail().getKey().getName());
		TaskConfig config= MemCache.PROC_MAP.get(xmlid);
		if (config == null) {
			LOG.error("can't found {} in TaskCfg",xmlid);
			return;
		}
		try {
			Date fireTime = context.getScheduledFireTime();
			
			//分钟级如果有延迟 需要对时间批次进行修正
			if("minute".equals(config.getRunFreq())) {
				CronExpression  cron = new CronExpression(config.getCronExp());
				//如果当前时间不符合 取上一个能匹配的触发事件
				if(!cron.isSatisfiedBy(fireTime)) {
					LOG.info("触发时间："+fireTime.toLocaleString());
					fireTime=CheckRunModelHandler.getConExpNearPastFireTimeMinute(config.getCronExp(), fireTime);
					LOG.info("延迟时间修正："+fireTime.toLocaleString());
				}
			}
			
			TaskLog newTl = ts.createTaskRunInfo(config, fireTime, null);
			if(newTl != null){
				LOG.info("timer[{},{}] triggered task:[{}]",config.getCronExp(),fireTime.toLocaleString(),newTl.getTaskId());
			}			
		} catch (Exception e) {
			LOG.error("timer[{}.{},{}] triggered task fail, will retry in 60s",config.getXmlid(),config.getProcName(),config.getCronExp());
			LOG.error(e.getMessage());
			try {
				Thread.sleep(60*1000L);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			this.execute(context);
		} 
	}
}
