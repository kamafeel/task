package com.asiainfo.dacp.scheduler.quartz;

import java.util.Calendar;
import java.util.Date;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskConfig;
import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.dp.server.scheduler.type.RunFreq;
import com.asiainfo.dacp.dp.tools.TimeUtils;
import com.asiainfo.dacp.scheduler.service.TaskService;


/**
 * 创建次周期任务为未触发状态,方便人为操作
 * @author zhangqi
 *
 */
@DisallowConcurrentExecution
public class CreatePlanJob implements Job {
	@Autowired
	private TaskService ts;
	
	private Logger LOG = LoggerFactory.getLogger(CreatePlanJob.class);

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		Date now = new Date();
		LOG.info("start doing create PlanJob of {}",TimeUtils.date2String(now, "yyyy-MM-dd"));		
		for (TaskConfig config : MemCache.PROC_MAP.values()) {
			try {
				switch (RunFreq.valueOf(config.getRunFreq())) {
				case day:// 日任务
					ts.createStandbyTaskRunInfo(config, now);
					break;
				case month:// 月任务
					//是否月末
					if(TimeUtils.isLast("day", TimeUtils.date2String(now, "yyyy-MM-dd"), false)){
						Calendar ca = Calendar.getInstance();
						ca.add(Calendar.MONTH, 0);
						ca.set(Calendar.DAY_OF_MONTH,1);
						ts.createStandbyTaskRunInfo(config, ca.getTime());
					}
					break;
				default:
					break;
				}
			} catch (Exception e) {
				LOG.error("create PlanJob of {} is fail", config.getXmlid(),e);
			}
		}
	}
}
