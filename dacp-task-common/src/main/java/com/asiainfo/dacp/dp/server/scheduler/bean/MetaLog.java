package com.asiainfo.dacp.dp.server.scheduler.bean;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/***
 * 输出表的日志表 proc_schedule_meta_log
 * 
 * @author wangyuanbin
 *
 */
@Getter
@Setter
public class MetaLog implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2453479796631439132L;
	private String seqno;
	private String xmlid;
	private String procName;
	private String procDate;
	private String target;
	private String dataTime;
	private Integer triggerFlag;
	private Integer needDqCheck;
	private Integer dqCheckRes;
	private String generateTime;
	private String dateArgs;
	/*** 流程号 */
	private String flowcode;

	public MetaLog() {
		triggerFlag = 1;
		needDqCheck = 0;
		dqCheckRes = 1;
	}

	public MetaLog clone() {
		MetaLog metaLog = null;
		ObjectOutputStream oo = null;
		ObjectInputStream oi = null;
		try {
			ByteArrayOutputStream bo = new ByteArrayOutputStream();
			oo = new ObjectOutputStream(bo);
			oo.writeObject(this);
			ByteArrayInputStream bi = new ByteArrayInputStream(bo.toByteArray());
			oi = new ObjectInputStream(bi);
			metaLog = (MetaLog) oi.readObject();
		} catch (Exception ex) {
		} finally {
			try {
				if (oo != null)
					oo.close();
				if (oi != null)
					oi.close();
			} catch (IOException e) {
			}
		}
		return metaLog;
	}

	public String getDataId() {
		StringBuilder dataId = new StringBuilder().append(procName).append(",").append(target).append(",")
				.append(dataTime);
		return dataId.toString();
	}
}
