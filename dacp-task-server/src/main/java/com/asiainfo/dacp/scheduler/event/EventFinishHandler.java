package com.asiainfo.dacp.scheduler.event;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.asiainfo.dacp.scheduler.service.TaskCacheService;
import com.lmax.disruptor.EventHandler;

/**
 * 事件完毕通知
 * @author zhangqi
 *
 */
@Service
public class EventFinishHandler implements EventHandler<TaskLogEvent>{
	@Autowired
	private TaskCacheService tcs;	
	
	@Override
	public void onEvent(TaskLogEvent event, long sequence, boolean endOfBatch) throws Exception {
		tcs.latch.countDown();	
	}	
}