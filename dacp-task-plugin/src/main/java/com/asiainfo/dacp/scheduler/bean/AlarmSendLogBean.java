package com.asiainfo.dacp.scheduler.bean;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class AlarmSendLogBean {
	private String id;
	private String xmlid;
	private String procName;
	private String dateArgs;
	private String phoneNumber;
	private String sendContent;
	private String sendTime;

}
