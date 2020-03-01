package com.asiainfo.dacp.dp.common;

public class RunStatus {
	/*** 重做 */
	public final static int REDO = 0;
	/*** 创建作业 */
	public final static int CREATE_TASK = 1;
	/*** 检查依赖成功 */
	public final static int CHECK_DEPEND_SUCCESS = 2;
	/*** 执行模式检测成功 */
	public final static int CHECK_RUNMODEL_SUCCESS = 3;
	/*** 发送Agent成功 */
	public final static int SEND_TO_MQ = 4;
	/*** 开始执行*/
	public final static int PROC_RUNNING = 5;
	/*** 执行成功 */
	public final static int PROC_RUN_SUCCESS = 6;
	/***计划任务*/
	public final static int PLAN_TASK= -7;
	/*** 执行失败 */
	public final static int PROC_RUN_FAIL = 51;
	/*** 异常信息error_code*/
	public final static class ERRCODE{
		/**同任务名的相同批次的任务在执行*/
		public final static int SAME = 201;
		/**同任务名的程序在执行*/
		public final static int MUT = 202;
		/**上一批次任务未执行*/
		public final static int PRE = 203;	
		/**未知异常*/
		public final static int EXCEPTION = 204;
		/**是否允许执行*/
		public final static int IS_ALLOW = 205;
		/**周期内上批次未执行*/
		public final static int CYCLE_PRE = 206;
		/**任务被暂停*/
		public final static int PAUSE = 207;
	}
	public final static class AGENT{
		/**agent 挂了*/
		public final static int DEAD = 301;
		/**agent 满了*/
		public final static int FULL = 302;
		/**找不到agent信息*/
		public final static int NEED = 303;
		/**未知异常*/
		public final static int EXCEPTION = 304;
		/**发送失败*/
		public final static int FAIL = 305;
	}
	//是否有效
	public final static class IsValid{
		public final static int VALID_FLAG=0;
	}
	
	public final static class AlarmType{
		/**到点未完成*/
		public final static int PROC_LATE = 1;
		/**程序错误*/
		public final static int PROC_ERROR = 2;
	}
}