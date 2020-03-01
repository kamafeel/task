package com.asiainfo.dacp.scheduler.event;

import com.asiainfo.dacp.dp.server.scheduler.bean.TaskLog;

/**
 * 
 * @author zhangqi
 *
 */
public class TaskLogEvent {

	private TaskLog tl;
	private int type; 

	public TaskLog getTl() {
		return tl;
	}

	public void setTl(TaskLog tl) {
		this.tl = tl;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}	
}