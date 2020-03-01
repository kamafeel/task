package com.asiainfo.dacp.scheduler.quartz;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbStorage;
import com.asiainfo.dacp.dp.tools.TimeUtils;

/**
 * 清理垃圾状态任务
 * @author zhangqi
 *
 */
@DisallowConcurrentExecution
public class ClearTranshTaskJob implements Job{
	
	@Autowired
	private TaskEventRdbStorage storage;
	@Value("${service.clear.trashTask.runTask.maxTime}")
	private int runTaskmaxTime;
	
	@Value("${service.clear.trashTask.planTask.maxTime}")
	private int planTaskmaxTime;
	
	@Value("${service.clear.trashTask.invaildTask.maxTime}")
	private int invaildTaskmaxTime;
	
	private Logger Log = LoggerFactory.getLogger(ClearTranshTaskJob.class);
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		Log.info("start doing clear TranshTask...");
		Date now = new Date();
		
		Calendar ca = Calendar.getInstance();
		ca.add(Calendar.DATE, -runTaskmaxTime);
		String rtmt = TimeUtils.date2String(ca.getTime(), "yyyy-MM-dd HH:mm:ss");
		
		ca.setTime(now);
		ca.add(Calendar.DATE, -planTaskmaxTime);
		String ptmt = TimeUtils.date2String(ca.getTime(), "yyyy-MM-dd HH:mm:ss");
		
		ca.setTime(now);
		ca.add(Calendar.DATE, -invaildTaskmaxTime);
		String itmt = TimeUtils.date2String(ca.getTime(), "yyyy-MM-dd HH:mm:ss");
		
		String[] sqlTextArr = new String[1];

		switch (MemCache.DBTYPE) {
		case MYSQL:
			sqlTextArr[0] = "DELETE FROM proc_schedule_log WHERE queue_flag=0 AND valid_flag=0 AND start_time<'" + rtmt + "'";
			sqlTextArr[1] = "DELETE FROM proc_schedule_log WHERE valid_flag=0 AND task_state=-7 AND start_time<'" + ptmt + "'";
			sqlTextArr[2] = "DELETE FROM proc_schedule_log WHERE valid_flag=1 AND start_time<'" + itmt + "'";
			break;
		case ORACLE:
			sqlTextArr[0] = "DELETE FROM proc_schedule_log WHERE queue_flag=0 AND valid_flag=0 AND start_time<'" + rtmt + "'";
			sqlTextArr[1] = "DELETE FROM proc_schedule_log WHERE valid_flag=0 AND task_state=-7 AND start_time<'" + ptmt + "'";
			sqlTextArr[2] = "DELETE FROM proc_schedule_log WHERE valid_flag=1 AND start_time<'" + itmt + "'";
			break;
		default:			
		}
		storage.sqlList2Transaction(Arrays.asList(sqlTextArr));
		Log.info("clear TranshTask is finish:{}",Arrays.asList(sqlTextArr));
	}

}
