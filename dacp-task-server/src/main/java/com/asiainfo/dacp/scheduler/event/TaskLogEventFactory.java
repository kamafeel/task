package com.asiainfo.dacp.scheduler.event;

import com.lmax.disruptor.EventFactory;

/**
 * 
 * @author zhangqi
 *
 */
public class TaskLogEventFactory implements EventFactory<TaskLogEvent>{

	@Override
	public TaskLogEvent newInstance() {
		 return new TaskLogEvent();
	}

}
