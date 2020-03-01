package com.asiainfo.dacp.dp.server.scheduler.bean;

import java.io.Serializable;

/**
 * proc_schedule_source_log表
 * @author wybhlm
 *
 */
public class SourceLog implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 9066705099199686494L;
	private String seqno;
	private String procName;
	private String source;
	private String sourceType;
	private String dataTime;
	private Integer checkFlag;
	/***流程号*/
	private String flowcode;
	private String dateArgs;
	private int level;
	private String cycleType;
	
	public SourceLog() {
		checkFlag = 0;
	}
	
	public String getSeqno() {
		return seqno;
	}
	public void setSeqno(String seqno) {
		this.seqno = seqno;
	}
	public String getProcName() {
		return procName;
	}
	public void setProcName(String procName) {
		this.procName = procName;
	}
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	public String getSourceType() {
		return sourceType;
	}
	public void setSourceType(String sourceType) {
		this.sourceType = sourceType;
	}
	public String getDataTime() {
		return dataTime;
	}
	public void setDataTime(String dataTime) {
		this.dataTime = dataTime;
	}
	public Integer getCheckFlag() {
		return checkFlag;
	}
	public void setCheckFlag(Integer checkFlag) {
		this.checkFlag = checkFlag;
	}
	public String getFlowcode() {
		return flowcode;
	}
	public void setFlowcode(String flowcode) {
		this.flowcode = flowcode;
	}
	public String getDateArgs() {
		return dateArgs;
	}
	public void setDateArgs(String dateArgs) {
		this.dateArgs = dateArgs;
	}
	public int getLevel() {
		return level;
	}
	public void setLevel(int level) {
		this.level = level;
	}
	public String getCycleType() {
		return cycleType;
	}
	public void setCycleType(String cycleType) {
		this.cycleType = cycleType;
	}
}