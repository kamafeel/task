package com.asiainfo.dacp.scheduler.quartz;

import java.io.File;
import java.util.List;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbStorage;
import com.asiainfo.dacp.dp.tools.file.FileUtil;

/**
 * 任务运行指标分析
 * @author zhangqi
 *
 */
@DisallowConcurrentExecution
public class TaskAnalysisJob implements Job{
	@Autowired
	private TaskEventRdbStorage storage;
	
	private Logger Log = LoggerFactory.getLogger(TaskAnalysisJob.class);
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		Log.info("start doing analysis of task");
		
		String dbType = "";
		switch (MemCache.DBTYPE) {
		case MYSQL:
			dbType = "mysql.sql";
			break;
		case ORACLE:
			dbType = "oracle.sql";
			break;
		default:			
		}
		
		try {
			List<String> sql = FileUtil.readLines(System.getProperty("user.dir") + File.separator + "sql" + File.separator + dbType);
			Log.info("sql analysis of task:{}",sql);
			storage.sqlList2Commit(sql);
		} catch (Exception e) {
			Log.error("analysis of task is fail",e);
		}
		Log.info("analysis of task is finish");
	}

}
