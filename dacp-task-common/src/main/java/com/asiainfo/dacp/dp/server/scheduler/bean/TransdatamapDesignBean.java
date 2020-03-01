package com.asiainfo.dacp.dp.server.scheduler.bean;

import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/**
 * 任务关系配置
 * @author zhangqi
 *
 */

@Getter
@Setter
public class TransdatamapDesignBean implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7861741175050525769L;
	/**源*/
	private String source;
	/**源类型*/
	private String sourcetype;
	/**源周期*/
	private String sourcefreq;
	/**程序*/
	private String target;
	/** 目标类型 */
	private String targettype;
	/** 目标周期 */
	private String targetfreq;
}
