package com.asiainfo.dacp.dp.server.scheduler.bean;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;


/***
 * proc_schedule_log
 * @author wybhlm
 *
 */
public class TaskLog implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5834285683149238416L;
	private String xmlid;
	/*** 任务批次号 */
	private String seqno;
	/*** 对象代码 */
	private String procName;
	/*** 任务状态 */
	private Integer taskState;
	/*** 状态时间 */
	private String statusTime;
	/*** 开始时间 */
	private String startTime;
	/*** 开始执行时间 */
	private String execTime;
	/*** 结束时间 */
	private String endTime;
	/*** 已经重做次数 */
	private Integer retryNum;
	/*** 日期参数 */
	private String dateArgs;
	/*** 程序日期 */
	private String procDate;
	/*** 触发标志，触发变为1 */
	private Integer triggerFlag;
	/*** 出队变为1 */
	private Integer queueFlag;
	/*** agent */
	private String agentCode;
	/***platform*/
	private String platform;
	/*** 优先级 */
	private Integer priLevel;
	/*** 任务类型周期 */
	private String runFreq;
	/*** 进程号*/
	private String pid;
	/***流程号*/
	private String flowcode;
	/*** 执行路径*/
	private String path;
	/*** 类型*/
	private String proctype;
	/*** 运行参数*/
	private String runpara;
	/**最迟完成时间*/
	private String  timeWin;
	/**错误代码*/
	private Integer errcode;
	/**是否有效*/
	private Integer validFlag;
	/**允许执行时间*/
	private String allowBeginTime;
	/**程序错误号*/
	private Integer returnCode;
	/** 运行时长 **/
	private String use_time;
	
	private Integer onOff;
	
	private String preCmd;
	private String teamCode;
	
	
	public Integer getOnOff() {
		return onOff;
	}

	public void setOnOff(Integer onOff) {
		this.onOff = onOff;
	}

	public String getPreCmd() {
		return preCmd;
	}

	public void setPreCmd(String preCmd) {
		this.preCmd = preCmd;
	}

	public String getUse_time() {
		return use_time;
	}

	public void setUse_time(String use_time) {
		this.use_time = use_time;
	}

	public Integer getReturnCode() {
		return returnCode;
	}

	public void setReturnCode(Integer returnCode) {
		this.returnCode = returnCode;
	}

	public String getAllowBeginTime() {
		return allowBeginTime;
	}

	public void setAllowBeginTime(String allowBeginTime) {
		this.allowBeginTime = allowBeginTime;
	}

	public Integer getValidFlag() {
		return validFlag;
	}

	public void setValidFlag(Integer validFlag) {
		this.validFlag = validFlag;
	}

	public String getTimeWin() {
		return timeWin;
	}

	public void setTimeWin(String timeWin) {
		this.timeWin = timeWin;
	}

	public TaskLog() {
		priLevel = 10;
		retryNum = 0;
		triggerFlag = 0;
		queueFlag = 0;
	}
	
	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getProctype() {
		return proctype;
	}

	public void setProctype(String proctype) {
		this.proctype = proctype;
	}

	public String getRunpara() {
		return runpara;
	}

	public void setRunpara(String runpara) {
		this.runpara = runpara;
	}

	public String getPid() {
		return pid;
	}

	public String getFlowcode() {
		return flowcode;
	}
	
	public void setFlowcode(String flowcode) {
		this.flowcode = flowcode;
	}
	public void setPid(String pid) {
		this.pid = pid;
	}
	public Integer getPriLevel() {
		return priLevel;
	}

	public void setPriLevel(Integer priLevel) {
		this.priLevel = priLevel;
	}

	public String getProcDate() {
		return procDate;
	}

	public String getAgentCode() {
		return agentCode;
	}

	public void setAgentCode(String agentCode) {
		this.agentCode = agentCode;
	}

	public void setProcDate(String procDate) {
		this.procDate = procDate;
	}

	public String getDateArgs() {
		return dateArgs;
	}

	public void setDateArgs(String dateArgs) {
		this.dateArgs = dateArgs;
	}

	public Integer getTaskState() {
			return taskState;
	}

	public void setTaskState(Integer taskState) {
			this.taskState = taskState;
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

	public Integer getTriggerFlag() {
			return triggerFlag;
	}

	public void setTriggerFlag(Integer triggerFlag) {
			this.triggerFlag = triggerFlag;
	}

	public Integer getQueueFlag() {
			return queueFlag;
	}

	public void setQueueFlag(Integer queueFlag) {
			this.queueFlag = queueFlag;
	}

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public String getEndTime() {
		return endTime;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

	public String getStatusTime() {
		return statusTime;
	}

	public void setStatusTime(String statusTime) {
		this.statusTime = statusTime;
	}

	public Integer getRetryNum() {
		return retryNum==null?0:retryNum;
	}

	public void setRetryNum(Integer retryNum) {
		this.retryNum = retryNum;
	}

	public String getExecTime() {
		return execTime;
	}

	public void setExecTime(String execTime) {
		this.execTime = execTime;
	}

	public String getRunFreq() {
		return runFreq;
	}

	public void setRunFreq(String runFreq) {
		this.runFreq = runFreq;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}
	public String getTaskId(){
		StringBuilder taskId = new StringBuilder("")
		.append(this.getSeqno()==null?"":this.getSeqno())
		.append(",")
		.append(this.getProcName()==null?"":this.getProcName())
		.append(",")
		.append(this.getDateArgs()==null?"":this.getDateArgs());
		return taskId.toString();
	}

	public Integer getErrcode() {
		return errcode;
	}
	public void setErrcode(Integer errcode) {
		this.errcode = errcode;
	}

	public String getXmlid() {
		return xmlid;
	}

	public void setXmlid(String xmlid) {
		this.xmlid = xmlid;
	}

	public String getTeamCode() {
		return teamCode;
	}

	public void setTeamCode(String teamCode) {
		this.teamCode = teamCode;
	}
	
	public TaskLog clone() {
		TaskLog taskLog = null;
		ObjectOutputStream oo = null;
		ObjectInputStream oi = null;
		try {
			ByteArrayOutputStream bo = new ByteArrayOutputStream();
			oo = new ObjectOutputStream(bo);
			oo.writeObject(this);
			ByteArrayInputStream bi = new ByteArrayInputStream(bo.toByteArray());
			oi = new ObjectInputStream(bi);
			taskLog = (TaskLog) oi.readObject();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (oo != null)
					oo.close();
				if (oi != null)
					oi.close();
			} catch (IOException e) {
			}
		}
		return taskLog;
	}

}
