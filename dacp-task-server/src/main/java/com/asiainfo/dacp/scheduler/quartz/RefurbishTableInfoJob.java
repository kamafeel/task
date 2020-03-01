package com.asiainfo.dacp.scheduler.quartz;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;

import com.asiainfo.dacp.scheduler.service.TaskService;
import com.asiainfo.dacp.scheduler.service.ZkService;

/**
 * 刷新表信息
 * @author zhangqi
 *
 */
@DisallowConcurrentExecution
public class RefurbishTableInfoJob implements Job {
	@Autowired
	private TaskService ts;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		ts.refurbishTableInfo();
	}
}
