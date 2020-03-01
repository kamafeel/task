package com.asiainfo.dacp.dp.message;

public class MapKeys {
	/***************************** 所有执行任务接收和发送字典 *****************************/

	/********************************* 接收任务字典 *********************************/
	/** 任务编号 */
	public final static String MSG_ID = "msgId";
	/** 任务类型 */
	public final static String MSG_TYPE = "msgType";// taskTypeProc,taskTypeFunc,taskTypeHeart;
	/** 日期参数 */
	public final static String PROC_DATE_VAR = "procDateVar";
	/** 执行状态 */
	public final static String PROC_STATUS = "procStatus";
	/** 执行状态描述 */
	public final static String PROC_LOG = "procLog";
	/** 进程pid */
	public final static String PROC_PID = "procPid";
	/**执行命令行 保留,以便向下兼容*/
	public final static String CMD_LINE = "commandLine";
	/**执行命令参数*/
	public final static String CMD_PARA = "cmdPara";
	/**执行命令名称*/
	public final static String CMD_NAME = "cmdName";
	/**执行命令前缀*/
	public final static String CMD_PRE = "cmdPre";
	/**dp程序出现错误步骤号*/
	public final static String PROC_RETURN_CODE="procReturnCode";
	/**日志编码*/
	public final static String LOG_FILE_CHARACTER_SET = "logFileCharacterSet";
	
	public static enum  VARTYPE{
		CONST,
		PRPAM,
		ALL
	}

	public static enum LogFileCharacterSet {
		UTF8("UTF-8"),
		GBK("GBK");

		private String value;

		LogFileCharacterSet(String value) {
			this.value = value;
		}

		public String getValue() {
			return value;
		}

		public static String getValue(int index) {
			for (LogFileCharacterSet charSet : LogFileCharacterSet.values()) {
				if (charSet.ordinal() == index) {
					return charSet.value;
				}
			}
			return null;
		}
	}
}
