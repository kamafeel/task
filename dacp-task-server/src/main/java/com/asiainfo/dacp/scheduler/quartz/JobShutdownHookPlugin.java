package com.asiainfo.dacp.scheduler.quartz;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.plugins.management.ShutdownHookPlugin;
import org.quartz.spi.ClassLoadHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 作业关闭钩子.
 *
 * @author zhangqi
 */
public final class JobShutdownHookPlugin extends ShutdownHookPlugin {
    
	private static Logger LOG = LoggerFactory.getLogger(JobShutdownHookPlugin.class);
	
    private String jobName;
    
    @Override
    public void initialize(final String name, final Scheduler scheduler, final ClassLoadHelper classLoadHelper) throws SchedulerException {
        super.initialize(name, scheduler, classLoadHelper);
        jobName = scheduler.getSchedulerName();
    }
    
    @Override
    public void shutdown() {
    	LOG.info("{} is shutdown,but i do nothing,just quietly wacth",jobName);
    }
}
