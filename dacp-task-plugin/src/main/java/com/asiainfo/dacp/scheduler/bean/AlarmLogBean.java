package com.asiainfo.dacp.scheduler.bean;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class AlarmLogBean {
	private String xmlid;
	private String procName;
	private String dateArgs;
	private String warningType;
	private int maxSendCnt;
	private int intervalTime;
	private int sendCnt;
	private String lastSendTime;
	private String smsGroupId;
}
