package com.asiainfo.dacp.dp.agent;

import java.util.ResourceBundle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

/**
 * 
 * @author MeiKefu
 * @modify zhangqi
 * @date 2017-12-18
 */
public class DpAgentBoot {
	
	private static Logger LOG = LoggerFactory.getLogger(DpAgentBoot.class);

	public static void main(String[] args) {
		try {
			ResourceBundle resource = ResourceBundle.getBundle("com/asiainfo/dacp/version");
			System.out.println(resource.getString("version"));
			
			String[] url = new String[] { "classpath*:conf/part/*.xml", "file:conf/*.xml" };
			if (args.length == 1) {
				url = new String[] { url[0], args[0] };
			}
			ApplicationContext context = new FileSystemXmlApplicationContext(url);
			DpAgentContext agentContext = context.getBean(DpAgentContext.class);
			agentContext.setAppContext(context);
			LOG.info("启动Agent:{}", agentContext.getAgentCode());
			LOG.info("注册Zk服务:{}",agentContext.getAzko().getServerLists());
			agentContext.getAzko().init().fillEphemeralAgentNode(agentContext.getAgentCode(), "live");
			LOG.info("启动消息侦听服务");
			agentContext.getDpReceiver().start();
			LOG.info("启动完毕");
		} catch (Throwable ex) {
			ex.printStackTrace();
			System.exit(-1);
		}
	}
}
