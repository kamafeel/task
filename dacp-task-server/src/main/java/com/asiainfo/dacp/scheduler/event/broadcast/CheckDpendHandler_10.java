package com.asiainfo.dacp.scheduler.event.broadcast;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.lmax.disruptor.EventHandler;
import com.asiainfo.dacp.scheduler.event.TaskLogEvent;
import com.asiainfo.dacp.scheduler.service.TaskService;

/**
 * 依赖检测
 * @author zhangqi
 *
 */
@Service
public class CheckDpendHandler_10 implements EventHandler<TaskLogEvent> {
	
	@Autowired
	private TaskService ts;
	
	@Override
	public void onEvent(TaskLogEvent event, long sequence, boolean endOfBatch) throws Exception {
		if((sequence % 10) == 9){
			ts.CheckDpend(event.getTl());
		}		
	}
}