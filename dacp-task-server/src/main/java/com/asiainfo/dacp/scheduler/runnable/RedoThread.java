package com.asiainfo.dacp.scheduler.runnable;

import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbSearch;
import com.asiainfo.dacp.scheduler.service.TaskService;

/**
 * 重做后续线程
 * @author zhangqi
 *
 */

public class RedoThread implements Runnable{
	
	private static Logger LOG = LoggerFactory.getLogger(RedoThread.class);
	
	private TaskEventRdbSearch search;
	private TaskService ts;
	private CountDownLatch latch;
	private String xmlid;
	private String dateArgs;
	
	public RedoThread(String xmlid,	String dateArgs,CountDownLatch latch, TaskService ts, TaskEventRdbSearch search){
		this.xmlid = xmlid;
		this.dateArgs = dateArgs;
		this.latch = latch;
		this.ts = ts;
		this.search = search;
	}
	
	@Override
	public  void run() {
		try {
			if(MemCache.PROC_MAP.containsKey(xmlid)){
				ts.redoTask(search.queryfollowUpTask(xmlid, dateArgs));
			}else{//输出数据类型
				ts.delete2Meta(xmlid,dateArgs);
			}
		} catch (Exception e) {
			LOG.error("redo fail", e);
		} finally {
			latch.countDown();
		}
	}
}
