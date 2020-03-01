package com.asiainfo.dacp.process;

import java.io.*;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 任务进程日志监控线程
 * @author zhangqi
 *
 */
public class ProcessLogMonitorCallable implements Callable<Boolean> {
	
	private Logger logger = LoggerFactory.getLogger(ProcessLogMonitorCallable.class);
	
	private Process _process;
	private String logFile;
	private String execText;
	private String msgId;
	private String charSet;
	
	public ProcessLogMonitorCallable(String msgId,Process _process,String logFile,String execText, String charSet){
		this.msgId = msgId;
		this._process = _process;
		this.logFile = logFile;
		this.execText = execText;
		this.charSet = charSet;
	}
	
	@Override
	public Boolean call() throws Exception {

		if (!_process.isAlive()) {
			logger.error("msgId:{} of Process is fast failed,i can't watch it",msgId);
			return true;
		}
		String line = null;
		BufferedWriter bw = null;
		BufferedReader out = new BufferedReader(new InputStreamReader(_process.getInputStream(), charSet));
		// 清空错误输出
		BufferedReader error = new BufferedReader(new InputStreamReader(_process.getErrorStream()));
		try {
			bw = new BufferedWriter(new FileWriter(logFile, true));
			bw.append(new String(execText.getBytes(), "UTF-8"));
			while ((line = out.readLine()) != null) {
				bw.append(new String(line.getBytes(), "UTF-8"));
				bw.newLine();
				bw.flush();
			}
			while ((line = error.readLine()) != null) {
			}
		} catch (Exception e) {
			logger.error("watch error",e);
		} finally {
			try {
				out.close();
				bw.close();
			} catch (Exception e) {
				logger.error("watch error",e);
			}
		}
		return true;
	}
}
