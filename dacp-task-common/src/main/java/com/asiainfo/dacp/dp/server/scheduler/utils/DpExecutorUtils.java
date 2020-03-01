package com.asiainfo.dacp.dp.server.scheduler.utils;

import java.io.StringWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import freemarker.template.Template;

public class DpExecutorUtils {
	
	private static Logger LOG = LoggerFactory.getLogger(DpExecutorUtils.class);
	
	public static String variableSubstitution(String text, Object data){
		StringWriter sw = new StringWriter();
		try {
			Template template = new Template(null,text,null);
			template.process(data, sw);
			LOG.debug("原始文本:{}，替换后:{}",text,sw);
		} catch (Exception e) {
			LOG.error("替换变量失败,{}",text,e);
			return text;
		}
		return sw.toString();
	}
}
