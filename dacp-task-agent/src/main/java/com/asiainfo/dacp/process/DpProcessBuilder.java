package com.asiainfo.dacp.process;

import com.asiainfo.dacp.dp.agent.DpAgentContext;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.WinNT.HANDLE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.*;
import java.lang.reflect.Field;
import java.util.concurrent.FutureTask;

@Component
public class DpProcessBuilder implements DpProcess {
	
	private Logger logger = LoggerFactory.getLogger(DpProcessBuilder.class);
	
	@Autowired
	private DpAgentContext context;

	private interface CLibrary extends Library {
		int getpid();

		int kill(int pid, int signal);
	}

	@Override
	public Process createProcess(final String logFile, String[] args, final String execText, final String msgId, String charSet) {
		Process process = null;
		try {
			this.mkdirs(logFile);
			ProcessBuilder builder = new ProcessBuilder(args);
			// builder.command(args);
			// 重定向输入输出
			builder.redirectErrorStream(true);
			process = builder.start();

			ProcessLogMonitorCallable pc = new ProcessLogMonitorCallable(msgId, process, logFile, execText, charSet);
			FutureTask<Boolean> oneTask = new FutureTask<Boolean>(pc);
			Thread oneThread = new Thread(oneTask);			
			oneThread.start();
			context.getFutureTaskMap().put(msgId, oneTask);

		} catch (Exception e) {
			String error = String.format("创建进程[%s}]失败，错误原因：[%s]", args, getExceptionDetail(e));
			logger.error(error,e);
			writeLog(logFile, error);
		}
		return process;
	}

	@Override
	public int getPid(Process process) {
		try {

			if (process == null) {
				return -1;
			}
			Field field = null;
			if (Platform.isWindows()) {
				try {
					field = process.getClass().getDeclaredField("handle");
					field.setAccessible(true);
					long _handle = field.getLong(process);
					HANDLE handle = new HANDLE();
					handle.setPointer(Pointer.createConstant(_handle));
					int pid = Kernel32.INSTANCE.GetProcessId(handle);
					return pid;
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			} else if (Platform.isMac()) {

			} else {
				try {
					field = process.getClass().getDeclaredField("pid");
					field.setAccessible(true);
					int pid = (Integer) field.get(process);
					return pid;
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return -1;
	}

	@Override
	public String getLog(String dstFile) {
		long mb = 1024 * 1024;
		StringBuilder logText = new StringBuilder();
		BufferedReader reader = null;
		try {
			File file = new File(dstFile);
			if ((file.length() / mb) > 10) {
				return logText.append("日志过大，请在服务器上查看").toString();
			}
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			while ((tempString = reader.readLine()) != null) {
				logText.append(tempString);
				logText.append(System.getProperty("line.separator"));
			}
		} catch (Exception e) {
			String log = String.format("读取文件%s失败", dstFile);
			logger.error(log,e);
			writeLog(dstFile, log);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
					// ignore
				}
			}
		}
		return logText.toString();
	}

	@Override
	public boolean kill(Process process, String shellPath) {
		if (process == null) {
			return true;
		}
		if (!isRun(process)) {
			return true;
		}
		Field field = null;
		if (Platform.isWindows()) {
			try {
				field = process.getClass().getDeclaredField("handle");
				field.setAccessible(true);
				long _handle = field.getLong(process);
				HANDLE handle = new HANDLE();
				handle.setPointer(Pointer.createConstant(_handle));
				return Kernel32.INSTANCE.TerminateProcess(handle, 1);
			} catch (Exception ex) {
				ex.printStackTrace();
				return false;
			}
		} else if (Platform.isMac()) {

		} else if (Platform.isLinux()) {
			try {
				field = process.getClass().getDeclaredField("pid");
				field.setAccessible(true);
				int pid = (Integer) field.get(process);
				Process ps = createProcess(new String[] { "/bin/sh", shellPath, String.valueOf(pid) });
				ps.waitFor();
				return true;
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}

		} else {
			try {
				field = process.getClass().getDeclaredField("pid");
				field.setAccessible(true);
				int pid = (Integer) field.get(process);
				Process ps = createProcess(new String[] { "/bin/sh", shellPath, String.valueOf(pid) });
				ps.waitFor();
				return true;
			} catch (Exception ex) {
				ex.printStackTrace();
				return false;
			}
		}
		return false;
	}

	@Override
	public boolean kill(Process process) {
		if (process != null) {
			process.destroy();
		}
		if (!isRun(process)) {
			return true;
		}
		Field field = null;
		if (Platform.isWindows()) {
			try {
				field = process.getClass().getDeclaredField("handle");
				field.setAccessible(true);
				long _handle = field.getLong(process);
				HANDLE handle = new HANDLE();
				handle.setPointer(Pointer.createConstant(_handle));
				return Kernel32.INSTANCE.TerminateProcess(handle, 1);
			} catch (Exception ex) {
				ex.printStackTrace();
				return false;
			}
		} else if (Platform.isMac()) {

		} else {
			try {
				field = process.getClass().getDeclaredField("pid");
				field.setAccessible(true);
				int pid = (Integer) field.get(process);
				CLibrary instace = (CLibrary) Native.loadLibrary("c", CLibrary.class);
				return 0 == instace.kill(pid, 9);
			} catch (Exception ex) {
				ex.printStackTrace();
				return false;
			}
		}
		return false;
	}

	@Override
	public boolean kill(int pid, int signal) {
		return false;
	}

	private boolean isRun(Process process) {
		if (process == null) {
			return false;
		}
		try {
			process.exitValue();
			return false;
		} catch (IllegalThreadStateException ex) {
			return true;
		} catch (Exception ex) {
			return true;
		}
	}

	private void mkdirs(String distPath) {
		if (!StringUtils.isEmpty(distPath)) {
			String pathFolder = distPath.substring(0, 1 + distPath.lastIndexOf(File.separator));
			File file = new File(pathFolder);
			if (!file.exists()) {
				file.mkdirs();
			}
//			File logFile = new File(distPath);
//			logFile.delete();
		}
	}

	/**
	 * 获取exception详情信息
	 * 
	 * @param e
	 *            Excetipn type
	 * @return String type
	 */
	private String getExceptionDetail(Exception e) {
		StringBuffer msg = new StringBuffer("null");
		if (e != null) {
			msg = new StringBuffer("");
			String message = e.toString();
			int length = e.getStackTrace().length;
			if (length > 0) {
				msg.append(message + "\n");
				for (int i = 0; i < length; i++) {
					msg.append("\t" + e.getStackTrace()[i] + "\n");
				}
			} else {
				msg.append(message);
			}

		}
		return msg.toString();
	}

	private void writeLog(String distPath, String content) {
		if (StringUtils.isEmpty(distPath) || StringUtils.isEmpty(content)) {
			return;
		}
		BufferedWriter writor = null;
		try {
			mkdirs(distPath);
			writor = new BufferedWriter(new FileWriter(distPath, true));
			writor.append(content + System.getProperty("line.separator"));
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("writeLog is error",e);
		} finally {
			if (writor != null) {
				try {
					writor.close();
				} catch (IOException e) {
				}
			}
		}
	}

	@Override
	public Process createProcess(String[] args) {
		Process process = null;
		ProcessBuilder builder = new ProcessBuilder();
		builder.command(args);
		try {
			process = builder.start();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("createProcess is error",e);
		}
		return process;
	}

}
