package com.asiainfo.dacp.dp.server.scheduler.bean;

import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/**
 * agent并发信息
 * 
 * @author wybhlm
 *
 */
@Getter
@Setter
public class AgentIps implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 2705293736659376214L;
	private String agentCode;
	private Integer ips;
	private Integer curips;
	private Integer agentStatus;
	private String scriptPath;
	private String statusChgtime;
	private String platform;
}
