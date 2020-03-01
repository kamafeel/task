package com.asiainfo.dacp.scheduler.quartz;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import com.asiainfo.dacp.scheduler.service.ZkService;

/**
 * 刷新agent配置
 * @author zhangqi
 *
 */
@DisallowConcurrentExecution
public class RefurbishAgentCfgJob implements Job {
	@Autowired
	private ZkService zs;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		zs.refurbishAgentConfig();
	}
}
