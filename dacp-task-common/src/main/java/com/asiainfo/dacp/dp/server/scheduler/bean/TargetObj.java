package com.asiainfo.dacp.dp.server.scheduler.bean;

import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/**
 * 输出表依赖配置信息
 * 
 * @author sufan
 *
 */
@Getter
@Setter
public class TargetObj implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2237021759936831642L;
	
	/** 程序或者数据 */
	private String source;
	/** 程序周期 */
	private String sourcefreq;
	/**源类型*/
	private String sourcetype;
	/** 目标 */
	private String target;
	/** 目标类型 */
	private String targettype;
	/** 目标周期 */
	private String targetfreq;
}
