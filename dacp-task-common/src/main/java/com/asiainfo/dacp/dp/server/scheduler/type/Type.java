package com.asiainfo.dacp.dp.server.scheduler.type;

public class Type {
	public enum RUN_TYPE{
		/***顺序执行[0]*/
		EXECUTE_SERIAL,
		/***多重启动[1]*/
		EXECUTE_MORE, 
		/***单一启动[2]*/
		EXECUTE_ONECE, 
		/***周期内顺序启动[3]*/
		EXECUTE_SERIAL_IN_CYCLE,
		/***常驻进程[4]*/
		EXECUTE_FOREVER;
	}
	/*** drive type */
	public enum DRIVE_TYPE {
		/*** 时间驱动 */
		TIME_TRIGGER,
		/*** 事件驱动 */
		EVENT_TRIGGER;
	}
}
