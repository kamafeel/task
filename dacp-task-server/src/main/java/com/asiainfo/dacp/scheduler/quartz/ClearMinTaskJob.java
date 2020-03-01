package com.asiainfo.dacp.scheduler.quartz;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.asiainfo.dacp.dp.common.RunStatus;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbStorage;
import com.asiainfo.dacp.dp.server.scheduler.type.RunFreq;

/**
 * 清理分钟级无效任务
 * @author zhangqi
 *
 */
@DisallowConcurrentExecution
public class ClearMinTaskJob implements Job{
	@Autowired
	private TaskEventRdbStorage storage;
	
	private Logger Log = LoggerFactory.getLogger(ClearMinTaskJob.class);
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		StringBuilder sql = new StringBuilder("delete from proc_schedule_log where run_freq='");
		sql.append(RunFreq.minute.name()).append("' and task_state=").append(RunStatus.PROC_RUN_SUCCESS).append(" and queue_flag=1 and trigger_flag=1");
		Log.info("start doing clear minute of task,sql:{}",sql.toString());
		storage.sql2Transaction(sql.toString());
		
		Log.info("clear minute of task is finish");
	}

}
