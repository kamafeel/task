package com.asiainfo.dacp.dp.server.scheduler.bean;

import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TaskGlobalVal implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 4877683009587006249L;
	private String varName;
	private String varType;
	private String varValue;
	private String memo;	
}
