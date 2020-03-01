package com.asiainfo.dacp.dp.server.scheduler.bean;

import java.io.Serializable;

/**
 * 任务配置信息
 * proc,proc_schedule_info
 * @author wybhlm
 *
 */
public class TaskConfig implements Serializable{
	/**
	 *
	 */
	private static final long serialVersionUID = -4166231476090198051L;
	private String xmlid;
	/*** 程序名[英文] */
	private String procName;

	/*** 触发类型*0--时间触发，1--事件触发 */
	private Integer triggerType;
	/*** agent组 */
	private String platform;
	/*** 程序运行周期：用于定义定时作业 */
	private String runFreq;
	/*** cron表达式 */
	private String cronExp;
	/**程序最大运行时长限制（单位：小时）*/
	private Integer maxRunHours;
	/*** 程序优先级 */
	private Integer priLevel;
	/*** 失败自动重做次数 */
	private Integer redoNum = 0;
	/*** 重做时间间隔 */
	private Integer redoInterval = 5;
	/*** 有效无效 */
	private Integer validFlag = 0;
	/*** 执行模式 */
	private Integer mutiRunFlag = 0;
	// 作业信息
	/*** agent代码 */
	private String agentCode;
	/** 脚本路径 */
	private String path;
	/*** 程序类型 */
	private String procType;
	/*** 日期参数 */
	private String dateArgs;
	/*** 时间窗口 */
	private String timeWin;
	/***有效期*/
	private String effTime;
	/***失效期*/
	private String expTime;
	/***状态*/
	private String state;
	/***组编号*/
	private String teamCode;
	/***流程号*/
	private String flowcode;
	/**关注度*/
	private Integer onFocus;
	/**执行程序名*/
	private String execProc;
	/**增加前置命令*/
	private String preCmd;
	/**日志编码*/
	private Integer characterSet;

	public Integer getOnFocus() {
		return onFocus;
	}
	public void setOnFocus(Integer onFocus) {
		if(onFocus==null)
			onFocus=0;
		else
			this.onFocus = onFocus;
	}
	public TaskConfig() {
		redoInterval = 5;
		mutiRunFlag = 1;
		priLevel = 1;
		redoNum = 0;
		validFlag = 1;
	}
	public String getProcName() {
		return procName;
	}
	public Integer getTriggerType() {
		return triggerType;
	}
	public String getPlatform() {
		return platform;
	}
	public String getRunFreq() {
		return runFreq;
	}
	public String getCronExp() {
		return cronExp;
	}
	public Integer getPriLevel() {
		return priLevel;
	}
	public Integer getRedoNum() {
		return redoNum==null?0:redoNum;
	}
	public Integer getRedoInterval() {
		return redoInterval;
	}
	public Integer getValidFlag() {
		return validFlag;
	}
	public Integer getMutiRunFlag() {
		return mutiRunFlag;
	}
	public String getAgentCode() {
		return agentCode;
	}
	public String getPath() {
		return path;
	}
	public String getProcType() {
		return procType;
	}
	public String getDateArgs() {
		return dateArgs;
	}
	public String getTimeWin() {
		return timeWin;
	}
	public String getEffTime() {
		return effTime;
	}
	public String getExpTime() {
		return expTime;
	}
	public String getState() {
		return state;
	}
	public String getTeamCode() {
		return teamCode;
	}
	public String getFlowcode() {
		return flowcode;
	}
	public void setProcName(String procName) {
		this.procName = procName;
	}
	public void setTriggerType(Integer triggerType) {
		this.triggerType = triggerType;
	}
	public void setPlatform(String platform) {
		this.platform = platform;
	}
	public void setRunFreq(String runFreq) {
		this.runFreq = runFreq;
	}
	public void setCronExp(String cronExp) {
		this.cronExp = cronExp;
	}
	public void setPriLevel(Integer priLevel) {
		this.priLevel = priLevel;
	}

	public void setRedoNum(Integer redoNum) {
		this.redoNum = redoNum;
	}
	public void setRedoInterval(Integer redoInterval) {
		this.redoInterval = redoInterval;
	}
	public void setValidFlag(Integer validFlag) {
		this.validFlag = validFlag;
	}
	public void setMutiRunFlag(Integer mutiRunFlag) {
		this.mutiRunFlag = mutiRunFlag;
	}
	public void setAgentCode(String agentCode) {
		this.agentCode = agentCode;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public void setProcType(String procType) {
		this.procType = procType;
	}
	public void setDateArgs(String dateArgs) {
		this.dateArgs = dateArgs;
	}
	public void setTimeWin(String timeWin) {
		this.timeWin = timeWin;
	}
	public void setEffTime(String effTime) {
		this.effTime = effTime;
	}
	public void setExpTime(String expTime) {
		this.expTime = expTime;
	}
	public void setState(String state) {
		this.state = state;
	}
	public void setTeamCode(String teamCode) {
		this.teamCode = teamCode;
	}
	public void setFlowcode(String flowcode) {
		this.flowcode = flowcode;
	}
	public String getXmlid() {
		return xmlid;
	}
	public void setXmlid(String xmlid) {
		this.xmlid = xmlid;
	}
	public String getExecProc() {
		return execProc;
	}
	public void setExecProc(String execProc) {
		this.execProc = execProc;
	}
	public String getPreCmd() {
		return preCmd;
	}
	public void setPreCmd(String preCmd) {
		this.preCmd = preCmd;
	}
	public Integer getMaxRunHours() {
		return maxRunHours;
	}
	public void setMaxRunHours(Integer maxRunHours) {
		this.maxRunHours = maxRunHours;
	}
	public Integer getCharacterSet() {
		return characterSet;
	}
	public void setCharacterSet(Integer characterSet) {
		this.characterSet = characterSet;
	}
}
