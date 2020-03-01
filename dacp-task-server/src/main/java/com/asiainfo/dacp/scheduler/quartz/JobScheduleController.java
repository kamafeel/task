
package com.asiainfo.dacp.scheduler.quartz;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.springframework.stereotype.Component;

/**
 * 作业调度控制器.
 * 
 * @author zhangqi
 */
@Component
public class JobScheduleController {
    
    private Scheduler scheduler;
        
    /**
     * 调度作业.
     * 
     * @param cron CRON表达式
     * @throws SchedulerException 
     */
    public void scheduleJob(String xmlid,String cron,Class<? extends Job> jobClass) throws SchedulerException {
    	JobDetail jobDetail = JobBuilder.newJob(jobClass).withIdentity(xmlid).build();
    	if (!getScheduler().checkExists(jobDetail.getKey())) {
            getScheduler().scheduleJob(jobDetail, createTrigger(xmlid,cron));
        }
    	if(!getScheduler().isStarted()){
    		getScheduler().start();
    	}      
    }
    
    /**
     * 删除作业
     * @param xmlid
     * @param cron
     * @param jobClass
     * @throws SchedulerException
     */
    public void delscheduleJob(String xmlid) throws SchedulerException {
    	JobKey jobKey = JobKey.jobKey(xmlid);
    	if (getScheduler().checkExists(jobKey)) {
            getScheduler().deleteJob(jobKey);
        }
    }
    
    /**
     * 重新调度作业.
     * 
     * @param cron CRON表达式
     * @throws SchedulerException 
     */
    public void rescheduleJob(String xmlid,String cron) throws SchedulerException {
    	CronTrigger trigger = (CronTrigger) getScheduler().getTrigger(TriggerKey.triggerKey(xmlid));
        if (!getScheduler().isShutdown() && null != trigger && !cron.equals(trigger.getCronExpression())) {
            getScheduler().rescheduleJob(TriggerKey.triggerKey(xmlid), createTrigger(xmlid,cron));
        }else if(!getScheduler().isShutdown() && null == trigger){
        	this.scheduleJob(xmlid, cron, QuartzJob.class);
        }
    }
    
    private CronTrigger createTrigger(String xmlid,String cron) {
        return TriggerBuilder.newTrigger().withIdentity(xmlid).withSchedule(CronScheduleBuilder.cronSchedule(cron).withMisfireHandlingInstructionFireAndProceed()).build();
    }
    
    /**
     * 暂停作业.
     * @throws SchedulerException 
     */
    public void pauseJob() throws SchedulerException {
    	if (!getScheduler().isShutdown()) {
            getScheduler().pauseAll();
        }
    }
    
    /**
     * 恢复作业.
     * @throws SchedulerException 
     */
    public void resumeJob() throws SchedulerException {
    	if (!getScheduler().isShutdown()) {
            getScheduler().resumeAll();
        }
    }
    
    /**
     * 立刻启动作业.
     * @throws SchedulerException 
     */
    public void triggerJob(String xmlid) throws SchedulerException {
    	if (!getScheduler().isShutdown()) {
            getScheduler().triggerJob(JobKey.jobKey(xmlid));
        }
    }
    
    /**
     * 关闭调度器.
     * @throws SchedulerException 
     */
    public void shutdown() throws SchedulerException {
    	if (!getScheduler().isShutdown()) {
            getScheduler().shutdown();
        }
    }

	public Scheduler getScheduler() {
		return scheduler;
	}

	public void setScheduler(Scheduler scheduler) {
		this.scheduler = scheduler;
	}
}
