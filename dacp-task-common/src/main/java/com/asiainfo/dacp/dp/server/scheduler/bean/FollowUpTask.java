package com.asiainfo.dacp.dp.server.scheduler.bean;

import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/**
 * 重做后续任务
 * @author zhangqi
 *
 */
@Getter
@Setter
public class FollowUpTask implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -5602333824764045393L;
	
	private String xmlid;
	private String dateArgs;
	private String sourceXmlid;
	private String sourceArgs;
	private int level;
}
