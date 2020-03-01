package com.asiainfo.dacp.dp.agent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.asiainfo.dacp.dp.common.RunStatus;
import com.asiainfo.dacp.dp.message.DpMessage;
import com.asiainfo.dacp.dp.message.MapKeys;
import com.asiainfo.dacp.execmodel.ScheduleExeInter;

/**
 * 
 * @author MeiKefu
 * @date 2014-12-18
 */
@Component
@Scope("prototype")
public class DpExecutorThread implements Runnable {
	private Logger logger = LoggerFactory.getLogger(DpExecutorThread.class);
	private Object message;
	@Autowired
	private DpAgentContext context;
	@Autowired
	private ScheduleExeInter schedulerExeInter;

	public DpExecutorThread build(Object message) {
		this.message = message;
		return this;
	}

	@Override
	public void run() {
		String execText = "";
		DpMessage msg = (DpMessage) message;
		DpMessage returnMsg = msg.clone();
		returnMsg.getFirstMap().clear();
		String cmdLine = null;
		try {
			String[] cmd = { msg.getFirstMap().get(MapKeys.CMD_PRE), msg.getFirstMap().get(MapKeys.CMD_NAME),
					msg.getFirstMap().get(MapKeys.CMD_PARA) };

			StringBuilder sbTmp = new StringBuilder();
			for (String s : cmd) {
				sbTmp.append(s).append(" ");
			}
			cmdLine = sbTmp.toString();

			schedulerExeInter.runcmd(msg, cmd, context);

		} catch (Exception e) {
			logger.error("agent run process fail", e);
			e.printStackTrace();
			execText += String.format("[%s,%s]执行错误：%s\r\n", msg.getMsgId(), cmdLine,
					DpAgentUtils.getExceptionDetail(e));
			returnMsg.getFirstMap().put(MapKeys.PROC_STATUS, "" + RunStatus.PROC_RUN_FAIL);
			returnMsg.getFirstMap().put(MapKeys.PROC_LOG, execText);
			context.offerSendQueue(returnMsg);
		}

	}
}
