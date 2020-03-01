package com.asiainfo.dacp.scheduler.alarm;

import javax.annotation.Resource;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 任务告警(重庆)
 * @author zhangqi
 *
 */
@DisallowConcurrentExecution
public class CqAlarmJob implements Job {
	
	@Resource(name = "jdbcTemplate")
	private JdbcTemplate jdbcTemplate;
	
	private Logger LOG = LoggerFactory.getLogger(CqAlarmJob.class);

	public void execute(JobExecutionContext context) throws JobExecutionException {
		LOG.info("OK!!!!!!!!!!!!!!!!!!!!!!!");
	}
}
