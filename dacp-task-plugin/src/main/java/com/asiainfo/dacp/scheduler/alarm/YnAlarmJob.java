package com.asiainfo.dacp.scheduler.alarm;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.Resource;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.scheduler.bean.AlarmLogBean;
import com.asiainfo.dacp.scheduler.bean.AlarmSendLogBean;

/**
 * 云南告警类
 * @author Rimon
 *
 */
@DisallowConcurrentExecution
public class YnAlarmJob implements Job {
	
	@Resource(name = "jdbcTemplate")
	private JdbcTemplate jdbcTemplate;
	
	private Logger LOG = LoggerFactory.getLogger(YnAlarmJob.class);

	public void execute(JobExecutionContext context) throws JobExecutionException {
		try {
			ProcessErrorProc();
			sendSMS();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void sendSMS() {
		// 未达到最大发送次数数据
		StringBuffer alarmSql = new StringBuffer(
				" SELECT a.xmlid, a.proc_name, a.date_args, a.last_send_time, a.interval_time, ")
						.append(" b.member_name, b.phone_number, a.send_cnt ")
						.append(" FROM proc_schedule_alarm_log a, sms_message_team_member b ")
						.append(" WHERE a.sms_group_id = b.team_id AND a.send_cnt < a.max_send_cnt AND b.sending_status =1");
		LOG.debug("======查询要处理的告警数据======");
		List<Map<String, Object>> alarmData = jdbcTemplate.queryForList(alarmSql.toString());
		try {
			for (Map<String, Object> map : alarmData) {
				LOG.debug("======遍历告警数据======");
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				Date nowTime = new Date();
				Date lastSendTime = sdf.parse(map.get("last_send_time").toString());
				Calendar ca = Calendar.getInstance();
				ca.setTime(lastSendTime);
				ca.add(Calendar.MINUTE, Integer.parseInt(map.get("interval_time").toString()));
				if (nowTime.after(ca.getTime())) {
					String content = map.get("proc_name") + "[" + map.get("date_args") + "]批次,执行失败，请核查！";
					String phoneNum = map.get("phone_number").toString();
					int sendCnt = Integer.parseInt(map.get("send_cnt").toString());
					String dateArgs = map.get("date_args").toString();
					String xmlid = map.get("xmlid").toString();
					String procName = map.get("proc_name").toString();
					sendCnt++;
					String sendTime = sdf.format(new Date());
					AlarmSendLogBean alarmSendLog = new AlarmSendLogBean();
					String id = UUID.randomUUID().toString();
					alarmSendLog.setId(id);
					alarmSendLog.setXmlid(xmlid);
					alarmSendLog.setProcName(procName);
					alarmSendLog.setDateArgs(dateArgs);
					alarmSendLog.setPhoneNumber(phoneNum);
					alarmSendLog.setSendContent(content);
					alarmSendLog.setSendTime(sendTime);
					this.insertAlarmSendLog(alarmSendLog);
					this.updateAlarmLog(sendCnt, sendTime, xmlid, dateArgs, "2");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	
		
	}

	private void ProcessErrorProc() throws Exception {
		// 查找执行失败告警数据
		StringBuffer sb = new StringBuffer("SELECT a.XMLID, a.PROC_NAME, c.date_args, a.INTERVAL_TIME, ")
				.append("a.MAX_SEND_CNT, a.WARNING_TYPE , 0 send_cnt, '1970-01-01 00:00:00' last_send_time, a.sms_group_id ")
				.append(" From sms_message_group_task a, proc_schedule_log c")
				.append(" WHERE a.XMLID = c.XMLID AND a.SMS_GROUP_ID = c.team_code AND c.VALID_FLAG = 0 AND c.task_state = 51")
				.append(" AND c.queue_flag = 1 AND a.IS_SEND =1 AND a.WARNING_TYPE = 2");
		List<AlarmLogBean> smsErrorData = jdbcTemplate.query(sb.toString(), new BeanPropertyRowMapper<AlarmLogBean>(AlarmLogBean.class));
		for (AlarmLogBean alarmLog : smsErrorData) {
			int ckRes = 0;
			String[] targetFields = new String[] { "xmlid", "date_args", "warning_type" };
			Object[] values = new Object[] { alarmLog.getXmlid(), alarmLog.getDateArgs(), alarmLog.getWarningType() };
			ckRes = checkExist("proc_schedule_alarm_log", targetFields, values);
			// 不存在就插入表
			if (ckRes == 0) {
				this.insertAlarmLog(alarmLog);
			}
		}
	}
	
	private int checkExist(String tableName, String[] fieldNames, Object[] values) {
		String sql = "select 1 num from " + tableName + " where ";
		for (int i = 0; i < fieldNames.length; i++) {
			if (i != fieldNames.length - 1) {
				sql += fieldNames[i] + "=? and ";
			} else {
				sql += fieldNames[i] + "=? " + getLimitStr();
			}
		}
		try {
			LOG.debug("exec-sql:{}", sql.toString());
			jdbcTemplate.queryForObject(sql, Integer.class, values);
			return 1;
		} catch (EmptyResultDataAccessException ex) {
			return 0;
		}
	}

	private String getLimitStr() {
		String res = "";
		switch (MemCache.DBTYPE) {
		case MYSQL:
			res += " limit 1";
			break;
		case ORACLE:
			res += " and rownum<2";
			break;
		default:
			break;
		}
		return res;
	}
	
	/**
	 *  插入proc_schedule_alarm_log表
	 * @param alarmLog
	 */
	private void insertAlarmLog(AlarmLogBean alarmLog) {
		//TODO 需要优化
		jdbcTemplate.update("insert into proc_schedule_alarm_log (xmlid,proc_name,date_args,warning_type,max_send_cnt,interval_time,send_cnt,last_send_time, sms_group_id) values (?,?,?,?,?,?,?,?,?)",
				alarmLog.getXmlid(),
				alarmLog.getProcName(),
				alarmLog.getDateArgs(),
				alarmLog.getWarningType(),
				alarmLog.getMaxSendCnt(),
				alarmLog.getIntervalTime(),
				alarmLog.getSendCnt(),
				alarmLog.getLastSendTime(),
				alarmLog.getSmsGroupId());
	}
	
	
	/**
	 * 更新proc_schedule_alarm_log中数据
	 * @param sendCnt
	 * @param lastSendTime
	 * @param xmlid
	 * @param dateArgs
	 */
	private void updateAlarmLog(int sendCnt, String lastSendTime, String xmlid, String dateArgs, String warningType) {
		String sql = "update proc_schedule_alarm_log set send_cnt=?, last_send_time=? where xmlid=? and date_args=? and warning_type=?";
		LOG.debug("exec-sql:{}", sql.toString());
		jdbcTemplate.update(sql, sendCnt, lastSendTime, xmlid, dateArgs, warningType);
	}
	
	/**
	 *  插入proc_schedule_alarm_send_log表
	 * @param alarmSendLog
	 */
	private void insertAlarmSendLog(AlarmSendLogBean alarmSendLog) {
		//TODO 需要优化
		jdbcTemplate.update("insert into proc_schedule_alarm_send_log (id,xmlid,proc_name,date_args,phone_number,send_content,send_time) values (?,?,?,?,?,?,?)",
				alarmSendLog.getId(),
				alarmSendLog.getXmlid(),
				alarmSendLog.getProcName(),
				alarmSendLog.getDateArgs(),
				alarmSendLog.getPhoneNumber(),
				alarmSendLog.getSendContent(),
				alarmSendLog.getSendTime());
	}


}
