package com.asiainfo.dacp.process;
/**
 * 进程执行器
 * @author wybhlm
 *
 */
public interface DpProcess {
	/**
	 * 创建进程
	 * @param logFile 日志文件路径
	 * @param args 参数
	 * @return
	 */
	Process  createProcess(String[] args);
	Process  createProcess(String logFile,String[] args,String execText,String msgId, String charSet);
	/**
	 * 获取pid
	 */
	int getPid(Process process);
	/**
	 * 获取日志
	 * @param logFile 日志文件
	 * @return 日志内容
	 */
	String getLog(String logFile);
	/**
	 * 杀进程
	 * @param process
	 * @return
	 */
	boolean kill(Process process,String shellPath);
	boolean kill(Process process);
	boolean kill(int pid,int signal);
}
