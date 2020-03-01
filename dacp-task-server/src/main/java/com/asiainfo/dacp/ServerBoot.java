package com.asiainfo.dacp;

import java.io.File;
import java.util.ResourceBundle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.asiainfo.dacp.dp.tools.ReflectSysClassLoader;
import com.asiainfo.dacp.dp.tools.TimeUtils;
import com.asiainfo.dacp.scheduler.service.TaskProcessService;

/**
 * @author zhangqi
 * @date 2017-06-27
 * @version 3.1.0
 */
public class ServerBoot {
	private static final Logger LOG = LoggerFactory.getLogger(ServerBoot.class);
	
	public static void main(String[] args) {
		try {
			if (args.length < 1) {
				System.out.println("useage like: sh startServer.sh %serverId%");
				System.exit(-1);
			}
			ResourceBundle resource = ResourceBundle.getBundle("com/asiainfo/dacp/version");
			System.out.println(resource.getString("version"));
			new ReflectSysClassLoader(System.getProperty("user.dir") + File.separator + "driver");
			new ReflectSysClassLoader(System.getProperty("user.dir") + File.separator + "plugin");
			ApplicationContext context = new FileSystemXmlApplicationContext(new String[] { "conf/*.xml" });
			context.getBean(TaskProcessService.class).doit(args[0],(args.length>=2 && TimeUtils.isNumeric(args[1]) ? Long.parseLong(args[1]) : 0l)
					,(args.length>=3 ? args[2] : null));
		} catch (Throwable ex) {
			LOG.error("The task-server has an exception, server down now.", ex);
			System.exit(-1);
		}
	}
}
