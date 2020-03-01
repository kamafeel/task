package com.asiainfo.dacp.scheduler.quartz;

import java.util.Properties;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.stereotype.Component;

/**
 * Quartz调度器工厂封装
 * @author zhangqi
 *
 */
@Component
public class TaskQuartzFactory {
	
	@Value("${org.quartz.threadPool.threadCount}")
	private String threadCount;
	@Value("${org.quartz.jobStore.misfireThreshold}")
	private String misfireThreshold;
	@Autowired
	private SchedulerFactoryBean schedulerFactory;
	
	private Scheduler scheduler;
	
	private Properties getBaseQuartzProperties() {
        Properties result = new Properties();
        result.put("org.quartz.threadPool.class", org.quartz.simpl.SimpleThreadPool.class.getName());
        result.put("org.quartz.threadPool.threadCount", threadCount);
        result.put("org.quartz.scheduler.instanceName", "DACP-TASK");
        result.put("org.quartz.jobStore.misfireThreshold", misfireThreshold);
        result.put("org.quartz.plugin.shutdownhook.class", JobShutdownHookPlugin.class.getName());
        result.put("org.quartz.plugin.shutdownhook.cleanShutdown", Boolean.TRUE.toString());
        return result;
    }
	
	public void createScheduler() throws SchedulerException {
		StdSchedulerFactory factory = new StdSchedulerFactory();
        factory.initialize(getBaseQuartzProperties());
        setScheduler(schedulerFactory.getScheduler());
    }

	public Scheduler getScheduler() {
		return scheduler;
	}

	public void setScheduler(Scheduler scheduler) {
		this.scheduler = scheduler;
	}
}
