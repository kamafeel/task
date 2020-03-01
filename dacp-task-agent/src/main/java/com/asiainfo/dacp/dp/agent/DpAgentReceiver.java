package com.asiainfo.dacp.dp.agent;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.asiainfo.dacp.dp.agent.service.ClearLogService;
import com.asiainfo.dacp.dp.agent.service.LogService;
import com.asiainfo.dacp.dp.common.RunStatus;
import com.asiainfo.dacp.dp.message.DpHandler;
import com.asiainfo.dacp.dp.message.DpMessage;
import com.asiainfo.dacp.dp.message.MsgType;
import com.asiainfo.dacp.dp.message.MapKeys;
import com.google.gson.Gson;


/**
 * 
 * @author MeiKefu
 * @date 2014-12-18
 */
public class DpAgentReceiver implements DpHandler {
	@Autowired
	private DpAgentContext context;
	@Autowired 
	private LogService logService;
	@Autowired 
	private ClearLogService clearLogService;
	private Logger logger = LoggerFactory.getLogger(DpAgentReceiver.class);


	public Object onMessage(Object object) {
		try {
			DpMessage msg = (DpMessage) object;
			logger.info("收到MQ消息:{}", new Gson().toJson(msg));
			DpMessage returnMsg = msg.clone();
			returnMsg.getFirstMap().clear();
			String msgId = msg.getMsgId();
			String msgType = StringUtils.trim(msg.getMsgType());
			String execText = "";
			// 参数校验
			if (StringUtils.isEmpty(msgId)) {
				execText = String.format("[%s]的消息不完整：编号为空", msgId);
			} else if (StringUtils.isEmpty(msgType)) {
				execText = String.format("[%s]的消息不完整：类型为空", msgId);
			} else {
				execText = "";
			}
			if (StringUtils.isNotEmpty(execText)) {
				returnMsg.getFirstMap().put(MapKeys.PROC_STATUS, "" + RunStatus.PROC_RUN_FAIL);
				returnMsg.getFirstMap().put(MapKeys.PROC_LOG, execText);
				context.offerSendQueue(returnMsg);
				return null;
			}
			switch (MsgType.valueOf(msgType)) {
			case taskTypeFunc:// 执行脚本
				DpExecutorThread dpExecutorThread = context.getAppContext().getBean(DpExecutorThread.class).build(msg);
				context.getThreadPool().execute(dpExecutorThread);
				return null;
			case KILL_PROC:// 杀进程
				Process killItem = context.getProcessMap().get(msgId);
				Boolean isSuccess = context.getDpProcess().kill(killItem, context.getShellPath());
				logger.info("kill task[{}] kill-shell[{}] {}", msgId, context.getShellPath(), isSuccess);
				if (isSuccess) {
					//context.getAzko().removeAgentRunTimeIfExisted(context.getAgentCode(), msgId);
					returnMsg = msg.clone();
					returnMsg.getFirstMap().clear();
					context.getDpSender().sendMessage(returnMsg.getSourceQueue(), returnMsg,-1);
				}
				return isSuccess.toString();
			case GET_LOG:
				return logService.service(msg, context);
			case CLEAR_LOG:
				return clearLogService.service(msg, context);
			default:
				return null;
			}
		} catch (Exception ex) {
			logger.error("", ex);
			return null;
		}
	}
	
}
