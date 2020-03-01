package com.asiainfo.dacp.scheduler.quartz;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbStorage;

/**
 * 上下线任务检测
 * @author zhangqi
 *
 */
@DisallowConcurrentExecution
public class CheckOnOffJob implements Job{
	@Autowired
	private TaskEventRdbStorage storage;
	
	private Logger Log = LoggerFactory.getLogger(CheckOnOffJob.class);
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		Log.info("start check OnOff");
		storage.OnOffCheck();		
		Log.info("check OnOff is finish");
	}

}
