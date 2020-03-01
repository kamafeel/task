package com.asiainfo.dacp.dp.server.scheduler.bean;

import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/**
 * 依赖表或者程序配置信息
 * @author wybhlm
 *
 */
@Getter
@Setter
public class SourceObj implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3557986742811364876L;
	
	/** 目标 */
	private String target;
	/**源*/
	private String source;
	/**源类型*/
	private String sourcetype;
	/**源周期*/
	private String sourcefreq;
}
