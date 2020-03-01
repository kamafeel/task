package com.asiainfo.dacp.execmodel;

import java.io.File;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import com.asiainfo.dacp.dp.agent.DpAgentContext;
import com.asiainfo.dacp.dp.agent.DpAgentUtils;
import com.asiainfo.dacp.dp.common.RunStatus;
import com.asiainfo.dacp.dp.message.DpMessage;
import com.asiainfo.dacp.dp.message.MapKeys;
import com.google.gson.Gson;

@Service
public class ScheduleExeInter {
	
	private Logger logger = LoggerFactory.getLogger(ScheduleExeInter.class);

	/**
	 * 常规脚本程序匹配通用执行方法
	 * 
	 * @param message
	 * @param cmdLine
	 */
	public void runcmd(com.asiainfo.dacp.dp.message.DpMessage message, String[] cmd, DpAgentContext context) {
		String execText = "";
		DpMessage returnMsg = message.clone();		
		returnMsg.getFirstMap().clear();
		
		String dateArgs = message.getDateArgs();
		String msgId = message.getMsgId();
		// 全能脚本不显示
		String cmdLine = (cmd != null && cmd.length == 3) ? cmd[2] : "";

		//execText += String.format("[%s]的执行参数：[%s]\r\n", msgId, cmdLine);
		//在调度日志内容中显示在哪个agent上执行该任务
		execText += String.format("[%s]在agent:[%s]的执行参数：[%s]\r\n", msgId,context.getAgentCode(), cmdLine);
		logger.info(execText);
		// 获取日志文件路径
		if (StringUtils.isEmpty(dateArgs)) {
			dateArgs = "otherLog";
		}
		//都是yyyy-MM-dd存放
		dateArgs = dateArgs.substring(0, 10);		
		try {
			StringBuilder log = new StringBuilder(context.getLogPath()).append(File.separator).append(dateArgs).append(File.separator).append(msgId).append(".log");
			String charSet = message.getFirstMap().get(MapKeys.LOG_FILE_CHARACTER_SET);
			if (charSet == null) {
				charSet = MapKeys.LogFileCharacterSet.UTF8.getValue();
			}
			// 执行命令
			Process process = context.getDpProcess().createProcess(log.toString(), cmd, execText,msgId, charSet);
			int pid = context.getDpProcess().getPid(process);

			logger.info("task:{},cmdLine:{} has pid:{}", msgId, cmdLine,pid);
			if (process != null) {
				context.getProcessMap().put(msgId, process);
				//更新ZK进程数
				context.getAzko().fillEphemeralAgentRunTime(context.getAgentCode(), message.getMsgId(),pid);
				logger.info("{},zk curips update", msgId);
			} else {
				logger.info("{},Process is null",msgId);
			}

			returnMsg.getFirstMap().put(MapKeys.PROC_STATUS, "" + RunStatus.PROC_RUNNING);
			returnMsg.getFirstMap().put(MapKeys.PROC_PID, "" + pid);
			context.offerSendQueue(returnMsg);
			logger.info("{},sendReturn mq",msgId);
			//TODO 需要优化,MQ消息是有序的,server是线程池处理消息,把消息变为无序,导致如果执行的程序极快的返回,导致正在运行和运行结束时间差极短(纳秒之间),导致任务状态混乱
			//BlockUtils.sleep(1000l);
			process.waitFor();
			logger.info("{},Process is finsh",msgId);
			if(context.getFutureTaskMap().get(msgId) != null){
				context.getFutureTaskMap().get(msgId).get();
				logger.info("{},log observer flush is finsh",msgId);
			}else{
				logger.error("{},log observer can't found",msgId);
			}	
			// 重新复制一份message;
			returnMsg = message.clone();
			returnMsg.getFirstMap().clear();
			/** 添加错误步骤号 */
			returnMsg.getFirstMap().put(MapKeys.PROC_RETURN_CODE, String.valueOf(process.exitValue()));
			if (process.exitValue() == 0) {
				returnMsg.getFirstMap().put(MapKeys.PROC_STATUS, "" + RunStatus.PROC_RUN_SUCCESS);
			} else {
				returnMsg.getFirstMap().put(MapKeys.PROC_STATUS, "" + RunStatus.PROC_RUN_FAIL);
			}
			logger.info("msgId:{},reply mq message:{}",msgId, new Gson().toJson(returnMsg));
			if(context.isRunLog2db()){
				returnMsg.getFirstMap().put(MapKeys.PROC_LOG, execText + context.getDpProcess().getLog(log.toString()));
			}
			context.offerSendQueue(returnMsg);
		} catch (Exception ex) {
			logger.error("hard process error", ex);
			String errLog = String.format("[%s,%s]执行错误：%s\r\n", msgId, cmdLine, DpAgentUtils.getExceptionDetail(ex));
			logger.info(errLog);
			returnMsg.getFirstMap().put(MapKeys.PROC_STATUS, "" + RunStatus.PROC_RUN_FAIL);
			context.offerSendQueue(returnMsg);
		} finally {
			context.getProcessMap().remove(msgId);
			context.getFutureTaskMap().remove(msgId);
			//更新ZK进程数
			context.getAzko().removeAgentRunTimeIfExisted(context.getAgentCode(), message.getMsgId());
		}
	}

}
