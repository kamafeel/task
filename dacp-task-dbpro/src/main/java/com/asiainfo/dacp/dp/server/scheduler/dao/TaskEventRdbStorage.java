package com.asiainfo.dacp.dp.server.scheduler.dao;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.ai.zq.common.util.env.IpUtils;
import com.asiainfo.dacp.dp.common.RunStatus;
import com.asiainfo.dacp.dp.server.scheduler.bean.MetaLog;
import com.asiainfo.dacp.dp.server.scheduler.bean.SourceLog;
import com.asiainfo.dacp.dp.server.scheduler.bean.SourceObj;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskLog;
import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.dp.server.scheduler.type.DataFreq;
import com.asiainfo.dacp.dp.server.scheduler.type.ObjType;
import com.asiainfo.dacp.dp.tools.TimeUtils;

/**
 * 
 * @author zhangqi
 *
 */
@Repository
public class TaskEventRdbStorage {

	@Resource(name = "jdbcTemplate")
	private JdbcTemplate jdbcTemplate;

	@Value("${paas.version}")
	private String paasVersion;

	private static Logger LOG = LoggerFactory
			.getLogger(TaskEventRdbStorage.class);

	/**
	 * 插入运行日志(2.0使用)
	 * 
	 * @param seqNo
	 * @param runStatus
	 */
	public void insertScriptLog(String seqNo, String content) {
		jdbcTemplate.update(
				"delete from proc_schedule_script_log where seqno=?", seqNo);
		LOG.info("content-length:{}", content.length());
		jdbcTemplate
				.update("insert into proc_schedule_script_log (seqno,generate_time,app_log) values(?,?,?)",
						seqNo, TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"),
						content);
	}

	/**
	 * 上下线检测
	 */
	public void OnOffCheck() {
		String now = TimeUtils.getCurrentTime("yyyy-MM-dd");
		if ("2".equals(paasVersion)) {
			StringBuffer sql = new StringBuffer(
					"SELECT t1.xmlid as xmlid FROM proc t1,proc_schedule_info t2 WHERE t1.xmlid=t2.xmlid AND t1.state ='INVALID' AND t2.eff_time<='")
					.append(now)
					.append("' AND t2.exp_time>='")
					.append(now)
					.append("'")
					.append("UNION SELECT t1.xmlid as xmlid FROM proc t1,proc_schedule_info t2 WHERE t1.xmlid=t2.xmlid AND t1.state ='VALID' AND (T2.eff_time>'")
					.append(now).append("' OR T2.exp_time<='").append(now)
					.append("')");
			LOG.debug("exec-sql:{}", sql.toString());
			List<String> xmlids = this.jdbcTemplate.queryForList(
					sql.toString(), String.class);
			for (String s : xmlids) {
				this.jdbcTemplate.update(
						"update proc set state='PUBLISHED' where xmlid=?", s);
			}
		} else {
			StringBuffer sql = new StringBuffer(
					"SELECT t1.tab_id as xmlid FROM dacp_meta_tab t1,proc_schedule_info t2 WHERE t1.tab_id=t2.xmlid AND t1.state ='INVALID' AND t2.eff_time<='")
					.append(now)
					.append("' AND t2.exp_time>='")
					.append(now)
					.append("'")
					.append("UNION SELECT t1.tab_id as xmlid FROM dacp_meta_tab t1,proc_schedule_info t2 WHERE t1.tab_id=t2.xmlid AND t1.state ='VALID' AND (T2.eff_time>'")
					.append(now).append("' OR T2.exp_time<='").append(now)
					.append("')");
			LOG.debug("exec-sql:{}", sql.toString());
			List<String> xmlids = this.jdbcTemplate.queryForList(
					sql.toString(), String.class);
			for (String s : xmlids) {
				this.jdbcTemplate
						.update("update dacp_meta_tab set state='PUBLISHED' where xmlid=?",
								s);
			}
		}
	}

	/**
	 * 更新任务优先级
	 * 
	 * @param seqNo
	 * @param runStatus
	 */
	public void updateTaskPriLevel(String seqNo, int priLevel) {
		String sql = "update proc_schedule_log set pri_level=? where seqno=? and queue_flag=0 and valid_flag=0";
		LOG.debug("exec-sql:{}", sql.toString());
		jdbcTemplate.update(sql, priLevel, seqNo);
	}

	/**
	 * 更新有效任务状态
	 * 
	 * @param seqNo
	 * @param runStatus
	 */
	public void updateTaskState(String seqNo, int runStatus) {
		String sql = "update proc_schedule_log set task_state=?,status_time=? where seqno=? and queue_flag=0 and valid_flag=0";
		LOG.debug("exec-sql:{}", sql.toString());
		jdbcTemplate.update(sql, runStatus,
				TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"), seqNo);
	}
	/**
	恢复任务
	**/
	public void recoverTask(String seqNo, int runStatus) {
		String sql = "update proc_schedule_log set queue_flag=0,task_state=?, status_time=? where seqno=? and valid_flag=0";
		LOG.debug("exec-sql:{}", sql.toString());
		jdbcTemplate.update(sql, runStatus,
				TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"), seqNo);
	}
	
	/**
	 * 重做任务当前/后续
	 * 
	 * @param seqNo
	 * @param runStatus
	 */
	public void updateTaskStateRedoCurOrAfter(String seqNo, boolean isAfter) {
		String sql = "update proc_schedule_log set task_state=?,status_time=?,trigger_flag=?,queue_flag=0 where seqno=? and valid_flag=0";
		LOG.debug("exec-sql:{}", sql.toString());
		jdbcTemplate.update(sql, RunStatus.REDO, TimeUtils
				.getCurrentTime("yyyy-MM-dd HH:mm:ss"), isAfter ? 0 : 1, seqNo);
	}

	/**
	 * 更新有效任务状态(强制执行,不考虑queue_flag)
	 * 
	 * @param seqNo
	 * @param runStatus
	 */
	public void updateTaskState2forcePass(String seqNo, int runStatus) {
		String sql = "update proc_schedule_log set task_state=?,status_time=?,queue_flag=0,trigger_flag=0 where seqno=? and valid_flag=0";
		LOG.debug("exec-sql:{}", sql.toString());
		jdbcTemplate.update(sql, runStatus,
				TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"), seqNo);
	}

	/**
	 * 更新有效任务状态,同时保持agentCode
	 * 
	 * @param seqNo
	 * @param runStatus
	 * @param agentCode
	 */
	public void updateTaskState2AgentCode(String seqNo, int runStatus,
			String agentCode) {
		String sql = "update proc_schedule_log set task_state=?,status_time=?,agent_code=? where seqno=? and queue_flag=0 and valid_flag=0 and task_state<?";
		LOG.debug("exec-sql:{}", sql.toString());
		jdbcTemplate.update(sql, runStatus,
				TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"), agentCode,
				seqNo, RunStatus.SEND_TO_MQ);
	}

	/**
	 * 更新PROC表状态
	 * 
	 * @param xmlid
	 * @param state
	 */
	public void updateProcState(String xmlid, String state) {
		String sql = "update proc set state=?,state_date=? where xmlid=?";
		LOG.debug("exec-sql:{}", sql.toString());
		jdbcTemplate.update(sql, state,
				TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"), xmlid);
	}

	/**
	 * 更新任务运行状态信息
	 * 
	 * @param seqNo
	 * @param pid
	 */
	public void updateTaskInRuning(String seqNo, String pid) {
//		String sql = "update proc_schedule_log set task_state=?,status_time=?,exec_time=?,pid=? where seqno=? and task_state=?";
		String sql = "update proc_schedule_log set task_state=?,status_time=?,exec_time=?,pid=? where seqno=? ";
		LOG.info("exec-sql:{}", sql.toString());
		jdbcTemplate.update(sql, RunStatus.PROC_RUNNING,
				TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"),
				TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"), pid, seqNo
				);
//				,RunStatus.SEND_TO_MQ);
	}

	/**
	 * 更新任务运行结果
	 * 
	 * @param seqNo
	 * @param runStatus
	 * @param returnCode
	 * @param queueFlag
	 * @param triggerFlag
	 */
	public void updateTaskRunFinish(String seqNo, int runStatus,
			int returnCode, int queueFlag, int triggerFlag, String execTime) {
		String sql = "update proc_schedule_log set task_state=?,status_time=?,end_time=?,return_code=?,queue_flag=?,trigger_flag=?,use_time=? where seqno=?";
		LOG.debug("exec-sql:{}", sql.toString());
		String use_time = "";
		try {
			use_time = StringUtils.isEmpty(execTime) ? "" : TimeUtils
					.formatLong(System.currentTimeMillis()
							- TimeUtils.string2Date(execTime,
									"yyyy-MM-dd HH:mm:ss").getTime());
		} catch (Exception e) {
			LOG.error("use_time", e);
		}
		jdbcTemplate.update(sql, runStatus,
				TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"),
				TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"), returnCode,
				queueFlag, triggerFlag, use_time, seqNo);
	}

	/**
	 * 任务出队,任务触发完毕后使用
	 * 
	 * @param seqNo
	 */
	public void setTaskQueueOut(String seqNo) {
		String sql = "update proc_schedule_log set status_time=?,trigger_flag=1,queue_flag=1 where seqno=?";
		LOG.debug("exec-sql:{}", sql.toString());
		jdbcTemplate.update(sql,
				TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"), seqNo);
	}

	/**
	 * 删除旧任务配置产生的运行记录
	 * 
	 * @param xmlid
	 */
	public void refurbishOldTaskLog2Delete(String xmlid) {
		String sql = "delete from proc_schedule_log where xmlid=? and queue_flag=0";
		LOG.debug("exec-sql:{}", sql.toString());
		jdbcTemplate.update(sql, xmlid);
	}

	/**
	 * 直接删除任务运行日志
	 * 
	 * @param seqno
	 */
	public void deleteTaskLog(String seqno) {
		String sql = "delete from proc_schedule_log where seqno=?";
		LOG.debug("exec-sql:{}", sql.toString());
		jdbcTemplate.update(sql, seqno);
		List<String> sqls = new ArrayList<String>();
		sqls.add("delete from proc_schedule_script_log where seqno='" + seqno
				+ "'");
		// sqls.add("delete from proc_schedule_meta_log where seqno='"+seqno +
		// "'");
		this.sqlList2Transaction(sqls);
	}

	/**
	 * 出队相关任务(异步方式)
	 * 
	 * @param seqno
	 */
	public void queueOutTaskLog2Transaction(String seqno) {
		List<String> sqls = new ArrayList<String>();
		sqls.add("update proc_schedule_log set queue_flag=1 where seqno='"
				+ seqno + "'");
		this.sqlList2Transaction(sqls);
	}

	/**
	 * 重置任务状态为依赖检测通过
	 * 
	 * @param seqNo
	 * @param retryNum
	 */
	public void resetTask2RunModel(String seqNo, int retryNum) {
		String sql = "update proc_schedule_log set task_state=?,status_time=?,trigger_flag=0,queue_flag=0,return_code=0,exec_time=null,end_time=null,retrynum=? where seqno=?";
		LOG.debug("exec-sql:{}", sql.toString());
		jdbcTemplate.update(sql, RunStatus.CHECK_DEPEND_SUCCESS,
				TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"), retryNum,
				seqNo);
	}

	/**
	 * 重置任务状态为执行模式检测通过
	 * 
	 * @param seqNo
	 * @param retryNum
	 */
	public void resetTask2Send2MQ(String seqNo, boolean resetAgentCode) {
		String sql = "update proc_schedule_log set task_state=?,status_time=?,exec_time=null,end_time=null where seqno=?";
		if (resetAgentCode) {
			sql = "update proc_schedule_log set task_state=?,status_time=?,exec_time=null,end_time=null,agent_code=null where seqno=?";
		}
		LOG.debug("exec-sql:{}", sql.toString());
		jdbcTemplate.update(sql, RunStatus.CHECK_RUNMODEL_SUCCESS,
				TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"), seqNo);
	}

	/**
	 * 任务运行超时,或者任务配置错误
	 * 
	 * @param seqNo
	 */
	public void setTaskError2QueueOut(String seqNo) {
		String sql = "update proc_schedule_log set task_state=?,status_time=?,end_time=?,trigger_flag=1,queue_flag=1 where seqno=?";
		LOG.debug("exec-sql:{}", sql.toString());
		jdbcTemplate.update(sql, RunStatus.PROC_RUN_FAIL,
				TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"),
				TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"), seqNo);
	}

	/**
	 * 任务运行日志出队并且前端不展示
	 * 
	 * @param seqNo
	 */
	public void setTaskInvalid(String seqNo) {
		String sql = "update proc_schedule_log set status_time=?,queue_flag=1,valid_flag=1 where seqno=?";
		LOG.debug("exec-sql:{}", sql.toString());
		jdbcTemplate.update(sql,
				TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"), seqNo);
		this.sql2Transaction("update proc_schedule_script_log set app_log=CONCAT(app_log,'\\n \\n[前置任务重做，此任务失效]') where seqno='"
				+ seqNo + "'");
	}

	/**
	 * Server高可用信息启动刷新
	 * 
	 * @param serverId
	 * @param status
	 * @param jettyPort
	 */
	public void addServerStatus(String serverId, int status, int jettyPort) {
		int exist = jdbcTemplate
				.update("update aietl_servernode set server_status=?,status_time=?,api_port=? where server_Id=?",
						status,
						TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"),
						jettyPort, serverId);
		if (exist == 0) {
			StringBuilder env = new StringBuilder(
					System.getProperty("user.name")).append("@").append(
					serverId.split("@_@")[1]);
			jdbcTemplate
					.update("insert into aietl_servernode (server_id,host_name,deploy_path,server_status,status_time,api_port) values (?,?,?,?,?,?)",
							serverId, env.toString(),
							System.getProperty("user.dir"), status,
							TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"),
							jettyPort);
		}
	}

	/**
	 * agent信息更新
	 * 
	 * @param agentId
	 * @param status
	 * @param curips
	 */
	public void updateAgentInfo(String agentId, String status, String curips) {
		StringBuilder sql = new StringBuilder(
				"update aietl_agentnode set status_chgtime='").append(
				TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss")).append("'");
		if (StringUtils.isNotEmpty(status)) {
			sql.append(",node_status='").append(status).append("'");
		}
		if (StringUtils.isNotEmpty(curips)) {
			sql.append(",curips='").append(curips).append("'");
		}
		sql.append(" where agent_name='").append(agentId).append("'");
		LOG.debug("exec-sql:{}", sql.toString());
		// jdbcTemplate.update(sql.toString());
//		MemCache.DB_EXECUTE_QUEUE.add(sql.toString());
		sql2Transaction(sql.toString());
	}

	/**
	 * Server高可用信息ZK刷新
	 * 
	 * @param serverId
	 * @param status
	 */
	public void updateServerStatus(List<String> serverIds, String serverIdLeader) {
		if (serverIds.isEmpty()) {
			return;
		}
		StringBuilder servers = new StringBuilder();
		for (String s : serverIds) {
			servers.append("'").append(s).append("',");
		}
		servers.deleteCharAt(servers.length() - 1);
		jdbcTemplate
				.update("update aietl_servernode set server_status=?,status_time=? where server_Id<>?",
						-1, TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"),
						serverIdLeader);
		jdbcTemplate
				.update("update aietl_servernode set server_status=?,status_time=? where server_Id in("
						+ servers + ") and server_Id<>?", 0,
						TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"),
						serverIdLeader);
		jdbcTemplate.update(
				"delete from aietl_servernode where server_status=?", -1);
	}

	/**
	 * 插入任务运行日志
	 * 
	 * @param runInfo
	 */
	public void insertTaskLog(TaskLog runInfo) {
		// TODO 需要优化
		jdbcTemplate
				.update("delete from proc_schedule_log where xmlid=? and date_args=? and task_state=?",
						runInfo.getXmlid(), runInfo.getDateArgs(),
						RunStatus.PLAN_TASK);
		jdbcTemplate
				.update("insert into proc_schedule_log (seqno,xmlid,proc_name,task_state,start_time,status_time,retrynum,date_args,proc_date,queue_flag,trigger_flag,agent_code,pri_level,platform,run_freq,team_code,time_win,flowcode,runpara,proctype,path,valid_flag,pre_cmd) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
						runInfo.getSeqno(), runInfo.getXmlid(),
						runInfo.getProcName(), runInfo.getTaskState(),
						runInfo.getStartTime(), runInfo.getStatusTime(),
						runInfo.getRetryNum(), runInfo.getDateArgs(),
						runInfo.getProcDate(), runInfo.getQueueFlag(),
						runInfo.getTriggerFlag(), runInfo.getAgentCode(),
						runInfo.getPriLevel(), runInfo.getPlatform(),
						runInfo.getRunFreq(), runInfo.getTeamCode(),
						runInfo.getTimeWin(), runInfo.getFlowcode(),
						runInfo.getRunpara(), runInfo.getProctype(),
						runInfo.getPath(), runInfo.getValidFlag(),
						runInfo.getPreCmd());
	}

	public void validTask(String xmlid, String dateArgs) {
		String sql = "update proc_schedule_log set queue_flag=1,valid_flag=1 where xmlid=? and date_args=? and valid_flag=0";
		jdbcTemplate.update(sql,xmlid,dateArgs);
	}
	/**
	 * 插入MetaLog输出表的日志表
	 * 
	 * @param runInfo
	 */
	public void insertMetaLog(MetaLog metaLog) {
		// TODO 需要优化
		jdbcTemplate
				.update("delete from proc_schedule_meta_log where target=? and data_time=?",
						metaLog.getTarget(), metaLog.getDataTime());
		jdbcTemplate
				.update("insert into proc_schedule_meta_log (seqno,proc_name,proc_date,target,data_time,trigger_flag,generate_time, flowcode,date_args) values (?,?,?,?,?,?,?,?,?)",
						metaLog.getSeqno(), metaLog.getProcName(),
						metaLog.getProcDate(), metaLog.getTarget(),
						metaLog.getDataTime(), metaLog.getTriggerFlag(),
						metaLog.getGenerateTime(), metaLog.getFlowcode(),
						metaLog.getDateArgs());
	}

	/**
	 * 删除事件
	 * 
	 * @param metaLog
	 */
	public void deleteMetaLog(MetaLog metaLog) {
		jdbcTemplate
				.update("delete from proc_schedule_meta_log where target=? and data_time=?",
						metaLog.getTarget(), metaLog.getDataTime());
	}

	/**
	 * 更新外部接口触发
	 * 
	 * @return
	 * @throws Exception
	 */
	public void updateTargetLog(MetaLog metaLog) {
		jdbcTemplate
				.update("update proc_schedule_target_log set trigger_flag=? where target=? and data_time=?",
						1, metaLog.getTarget(), metaLog.getDataTime());
	}

	/**
	 * 批量插入依赖检测表
	 * 
	 * @param ms
	 * @param seqNo
	 * @param dateArgs
	 * @param runFreq
	 * @throws Exception
	 */
	public void insertSourceLog(Map<String, SourceObj> ms, String seqNo,
			String xmlid, String dateArgs) throws Exception {
		if (ms == null || ms.isEmpty()) {
			return;
		}
		String key = xmlid + "@" + dateArgs;
		if (MemCache.SOURCE_EVENT.containsKey(key)) {
			MemCache.SOURCE_EVENT.remove(key);
		}

		final List<SourceLog> srcLogList = new ArrayList<SourceLog>();
		Set<Map.Entry<String, SourceObj>> set = ms.entrySet();
		Iterator<Map.Entry<String, SourceObj>> it = set.iterator();
		while (it.hasNext()) {
			Map.Entry<String, SourceObj> item = it.next();
			SourceObj obj = item.getValue();
			if (obj.getSourcefreq().equals(DataFreq.N.name())) {
				continue;
			}
			SourceLog srcLog = new SourceLog();
			srcLog.setSeqno(seqNo);
			srcLog.setProcName(obj.getTarget());
			srcLog.setDateArgs(dateArgs);
			srcLog.setSource(obj.getSource());
			srcLog.setSourceType(obj.getSourcetype());
			srcLog.setDataTime(TimeUtils.getDependDateArgs(obj.getSourcefreq(),
					dateArgs, StringUtils.equals(obj.getSourcetype(),
							ObjType.PROC.name())));
			srcLog.setCheckFlag(0);
			srcLogList.add(srcLog);
		}

		BatchPreparedStatementSetter pss = new BatchPreparedStatementSetter() {
			public void setValues(PreparedStatement ps, int i)
					throws SQLException {
				SourceLog sourceLog = srcLogList.get(i);
				ps.setString(1, sourceLog.getSeqno());
				ps.setString(2, sourceLog.getProcName());
				ps.setString(3, sourceLog.getSource());
				ps.setString(4, sourceLog.getSourceType());
				ps.setString(5, sourceLog.getDataTime());
				ps.setInt(6, sourceLog.getCheckFlag());
				ps.setString(7, sourceLog.getFlowcode());
				ps.setString(8, sourceLog.getDateArgs());
			}

			public int getBatchSize() {
				return srcLogList.size();
			}
		};
		this.jdbcTemplate
				.batchUpdate(
						"insert into proc_schedule_source_log (seqno,proc_name,source,source_type,data_time,check_flag, flowcode,date_args) values(?,?,?,?,?,?,?,?)",
						pss);
	}

	/**
	 * 插入proc_schedule_target_log(重庆专用)
	 * 
	 * @param metaList
	 */
	public void insertTargertLog(final List<MetaLog> metaList) {
		BatchPreparedStatementSetter pss = new BatchPreparedStatementSetter() {
			public void setValues(PreparedStatement ps, int i)
					throws SQLException {
				MetaLog metaLog = metaList.get(i);
				ps.setString(1, metaLog.getSeqno());
				ps.setString(2, metaLog.getProcName());
				ps.setString(3, metaLog.getProcDate());
				ps.setString(4, metaLog.getTarget());
				ps.setString(5, metaLog.getDataTime());
				ps.setInt(6, metaLog.getTriggerFlag());
				ps.setString(
						7,
						StringUtils.isEmpty(metaLog.getGenerateTime()) ? TimeUtils
								.getCurrentTime("yyyy-MM-dd HH:mm:ss")
								: metaLog.getGenerateTime());
				ps.setString(8, metaLog.getFlowcode());
				ps.setString(9, metaLog.getDateArgs());
			}

			public int getBatchSize() {
				return metaList.size();
			}
		};
		this.jdbcTemplate
				.batchUpdate(
						"insert into proc_schedule_target_log (seqno,proc_name,proc_date,target,data_time,trigger_flag,generate_time, flowcode,date_args) values(?,?,?,?,?,?,?,?,?)",
						pss);
	}

	/**
	 * 异步批量更新
	 * 
	 * @param tableName
	 * @param parameter
	 * @param condi
	 */
	public void update2Transaction(String tableName,
			Map<String, Object> parameter, Map<String, Object> condi) {
		if (StringUtils.isEmpty(tableName)) {
			LOG.error("表明为空！");
			return;
		}
		if (parameter.isEmpty()) {
			LOG.error("更新参数为空！");
			return;
		}
		if (condi.isEmpty()) {
			LOG.error("更新条件为空！");
			return;
		}
		condi = cleanMapNull(condi);
		parameter = cleanMapNull(parameter);
		String sql = " update " + tableName + " set ";
		int index = 0;
		for (String key : parameter.keySet()) {
			index++;
			if (!StringUtils.isEmpty(parameter.get(key) == null ? ""
					: parameter.get(key).toString())) {
				String value = parameter.get(key).toString();
				value = value.replaceAll("\'+", "\'\'");
				if (index == parameter.keySet().size()) {
					sql += " " + key + "= '" + value + "'";
				} else {
					sql += " " + key + "= '" + value + "',";
				}
			}
		}
		sql += " where 1=1 ";
		for (String key : condi.keySet()) {
			if (!StringUtils.isEmpty(condi.get(key) == null ? "" : condi.get(
					key).toString())) {
				String value = condi.get(key).toString();
				value = value.replaceAll("\'+", "\'\'");
				sql += " and " + key + "='" + value + "'";
			}
		}
//		MemCache.DB_EXECUTE_QUEUE.add(sql);
		sql2Transaction(sql);
	}

	/**
	 * 异步批量插入
	 * 
	 * @param tableName
	 * @param parameter
	 */
	public void insert2Transaction(String tableName,
			Map<String, Object> parameter) {
		if (StringUtils.isEmpty(tableName)) {
			LOG.error("表名为空！");
			return;
		}
		if (parameter.isEmpty()) {
			LOG.error("更新参数为空！");
			return;
		}
		StringBuffer sb = new StringBuffer();
		String sql = " insert into " + tableName + "(";
		int index = 0;
		Map<String, Object> parameters = cleanMapNull(parameter);
		for (String key : parameters.keySet()) {
			index++;
			if (!StringUtils.isEmpty(parameters.get(key) == null ? ""
					: parameters.get(key).toString())) {
				String value = parameters.get(key).toString();
				value = value.replaceAll("\'+", "\'\'");
				if (index != parameters.keySet().size()) {
					sql += "" + key + ",";
					sb.append("'" + value + "',");
				} else {
					sql += "" + key;
					sb.append("'" + value + "'");
				}
			}
		}
		sql += ") values({})";
		sql = sql.replace("{}", sb.toString());

//		MemCache.DB_EXECUTE_QUEUE.add(sql);
		sql2Transaction(sql);
	}

	/**
	 * 异步批量删除
	 * 
	 * @param tableName
	 * @param condi
	 */
	public void delete2Transaction(String tableName, Map<String, Object> condi) {
		if (StringUtils.isEmpty(tableName)) {
			LOG.error("表明为空！");
			return;
		}
		if (condi.isEmpty()) {
			LOG.error("删除条件为空！");
			return;
		}
		String sql = "delete from " + tableName + " where 1=1";
		condi = cleanMapNull(condi);
		for (String key : condi.keySet()) {
			if (!StringUtils.isEmpty(condi.get(key) == null ? "" : condi.get(
					key).toString())) {
				String value = condi.get(key).toString();
				value = value.replaceAll("\'+", "\'\'");
				sql += " and " + key + "='" + value + "'";
			}
		}
//		MemCache.DB_EXECUTE_QUEUE.add(sql);
		sql2Transaction(sql);
	}

	/**
	 * 事务单条提交
	 * 
	 * @param sql
	 */
	public void sql2Transaction(String sql) {
		if (StringUtils.isNoneEmpty(sql)&&MemCache.DB_EXECUTE_QUEUE_DICTIONARY.get(sql)==null) {
			MemCache.DB_EXECUTE_QUEUE.add(sql);
			MemCache.DB_EXECUTE_QUEUE_DICTIONARY.put(sql, "1");
		}
	}

	/**
	 * 事务多条提交
	 * 
	 * @param sqls
	 */
	public void sqlList2Transaction(List<String> sqls) {
		if (!sqls.isEmpty()) {
//			MemCache.DB_EXECUTE_QUEUE.addAll(sqls);
			for(String sql : sqls) {
				if (StringUtils.isNoneEmpty(sql)&&MemCache.DB_EXECUTE_QUEUE_DICTIONARY.get(sql)==null) {
					MemCache.DB_EXECUTE_QUEUE.add(sql);
					MemCache.DB_EXECUTE_QUEUE_DICTIONARY.put(sql, "1");
				}
			}
		}
	}

	/**
	 * 批量非事务提交
	 * 
	 * @param sqls
	 */
	public void sqlList2Commit(List<String> sqls) {
		if (!sqls.isEmpty()) {
			String[] sql = new String[sqls.size()];
			this.jdbcTemplate.batchUpdate(sqls.toArray(sql));
		}
	}

	/**
	 * 内部方法
	 * 
	 * @param parameter
	 * @return
	 */
	private static Map<String, Object> cleanMapNull(
			Map<String, Object> parameter) {
		Iterator<String> iterator = parameter.keySet().iterator();
		while (iterator.hasNext()) {
			String key = iterator.next();
			if (StringUtils.isEmpty(parameter.get(key) == null ? "" : parameter
					.get(key).toString())) {
				iterator.remove();
			}
		}
		return parameter;
	}
}
