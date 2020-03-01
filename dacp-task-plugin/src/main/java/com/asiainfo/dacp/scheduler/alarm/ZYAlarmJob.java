package com.asiainfo.dacp.scheduler.alarm;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Resource;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.scheduler.bean.AlarmLogBean;
import com.asiainfo.dacp.scheduler.bean.AlarmSendLogBean;
import com.asiainfo.dacp.scheduler.storage.AlarmStorage;
import com.wisentsoft.service.sms.gsmp.exception.ConfigFileNotFoundException;
import com.wisentsoft.service.sms.gsmp.exception.ConnectionAreadyConnectedException;
import com.wisentsoft.service.sms.gsmp.proxy.GSMPProxy;

/**
 * 任务告警(中邮)
 * 
 * @author Rimon
 *
 */
@DisallowConcurrentExecution
public class ZYAlarmJob implements Job {

	@Resource(name = "jdbcTemplate")
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private AlarmStorage storage;

	private Logger LOG = LoggerFactory.getLogger(ZYAlarmJob.class);

	private String ip;
	private int port;
	private String user;
	private String passwd;
	private String extCode;

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		Properties property = new Properties();
		InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("gsmpClient.properties");
		try {
			property.load(inputStream);
			ip = property.getProperty("gsmp.ip");
			port = Integer.parseInt(property.getProperty("gsmp.port"));
			user = property.getProperty("gsmp.user");
			passwd = property.getProperty("gsmp.pwd");
			extCode = property.getProperty("gsmp.code");
			ProcessErrorProc();
			sendSMS();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void ProcessErrorProc() throws Exception {
		// 查找执行失败告警数据
		StringBuffer sb = new StringBuffer("SELECT a.XMLID, a.PROC_NAME, c.date_args, a.INTERVAL_TIME, ")
				.append("a.MAX_SEND_CNT, a.WARNING_TYPE , 0 send_cnt, '' last_send_time, a.sms_group_id ")
				.append(" From sms_message_group_task a, proc_schedule_log c")
				.append("WHERE a.XMLID = c.XMLID AND c.VALID_FLAG = 0 AND c.task_state = 51")
				.append(" AND c.queue_flag = 1 AND a.IS_SEND =1 AND a.WARNING_TYPE = 2");
		List<AlarmLogBean> smsErrorData = jdbcTemplate.query(sb.toString(), new BeanPropertyRowMapper<AlarmLogBean>());
		for (AlarmLogBean alarmLog : smsErrorData) {
			int ckRes = 0;
			String[] targetFields = new String[] { "xmlid", "date_args", "warning_type" };
			Object[] values = new Object[] { alarmLog.getXmlid(), alarmLog.getDateArgs(), alarmLog.getWarningType() };
			ckRes = checkExist("proc_schedule_alarm_log", targetFields, values);
			// 不存在就插入表
			if (ckRes == 0) {
				storage.insertAlarmLog(alarmLog);
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

	private void sendSMS() {

		GSMPProxy proxy = new GSMPProxy();
		try {
			System.out.println(ip + "   " + user + "  " + port + "  " + passwd);
			proxy.connect(ip, port, user, passwd, 30, "./src/main/java/wisent_log.properties");
		} catch (ConfigFileNotFoundException e1) {
			e1.printStackTrace();
			return;
		} catch (IllegalArgumentException e1) {
			e1.printStackTrace();
			return;
		} catch (ConnectionAreadyConnectedException e1) {
			e1.printStackTrace();
		}
		new SubmitThread(proxy).start();
	}

	class SubmitThread extends Thread {
		private GSMPProxy proxy;

		public SubmitThread(GSMPProxy proxy) {
			this.proxy = proxy;
		}

		public void run() {
			// 未达到最大发送次数数据
			StringBuffer alarmSql = new StringBuffer(
					" SELECT a.xmlid, a.proc_name, a.date_args, a.last_send_time, a.interval_time ")
							.append(" b.member_name, b.phone_number ")
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
						int sendCnt = Integer.parseInt(map.get("sendCnt").toString());
						String dateArgs = map.get("date_args").toString();
						String xmlid = map.get("xmlid").toString();
						String procName = map.get("proc_name").toString();
						LOG.debug("======发送短信======");
						String res = proxy.submitMTSMS(phoneNum, content, extCode);
						// 发送成功
						if (res != null && "".equals(res)) {
							LOG.debug("======发送成功，更新告警表数据======");
							sendCnt++;
							String sendTime = sdf.format(new Date());
							storage.updateAlarmLog(sendCnt, sendTime, xmlid, dateArgs, "2");
							AlarmSendLogBean alarmSendLog = new AlarmSendLogBean();
							alarmSendLog.setXmlid(xmlid);
							alarmSendLog.setProcName(procName);
							alarmSendLog.setDateArgs(dateArgs);
							alarmSendLog.setPhoneNumber(phoneNum);
							alarmSendLog.setSendContent(content);
							alarmSendLog.setSendTime(sendTime);
							storage.insertAlarmSendLog(alarmSendLog);
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				LOG.debug("======11111======");
				try {
					proxy.reConnect();
				} catch (ConnectionAreadyConnectedException e1) {
					e1.printStackTrace();
					LOG.debug("======22222======");
				}
			}
		}
	}
}
