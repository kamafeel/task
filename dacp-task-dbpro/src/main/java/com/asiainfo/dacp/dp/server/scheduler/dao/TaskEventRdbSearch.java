package com.asiainfo.dacp.dp.server.scheduler.dao;

import java.util.List;

import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.asiainfo.dacp.dp.common.RunStatus;
import com.asiainfo.dacp.dp.server.scheduler.bean.AgentIps;
import com.asiainfo.dacp.dp.server.scheduler.bean.MetaLog;
import com.asiainfo.dacp.dp.server.scheduler.bean.RunPara;
import com.asiainfo.dacp.dp.server.scheduler.bean.SourceLog;
import com.asiainfo.dacp.dp.server.scheduler.bean.TableInfo;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskConfig;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskGlobalVal;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskLog;
import com.asiainfo.dacp.dp.server.scheduler.bean.TransdatamapDesignBean;
import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.dp.tools.TimeUtils;

/**
 * 数据查询访问类
 * @author zhangqi
 *
 */
@Repository
public class TaskEventRdbSearch {
	
	@Resource(name = "jdbcTemplate")
	private JdbcTemplate jdbcTemplate;
	
	private static Logger LOG = LoggerFactory.getLogger(TaskEventRdbSearch.class);
	
	
	/**
	 * 获取table信息
	 * @return
	 */
	public List<TableInfo> queryTableInfo(String version) {
		String sql = null;
		switch (version) {
		case "2":
			sql = "SELECT DISTINCT xmlid AS id,dataname AS NAME,DATACNNAME AS cnName,CYCLETYPE AS cycleType FROM tablefile";
			break;
		case "3":
			sql = "SELECT DISTINCT a.tab_id AS id,a.tab_name AS NAME,a.tab_label AS cnName,a.cycle_type AS cycleType FROM dacp_meta_tab a, dacp_meta_datasource b WHERE a.ds_id = b.ds_id";
			break;
		default:
		}
		return this.jdbcTemplate.query(sql, new BeanPropertyRowMapper<TableInfo>(TableInfo.class));
	}
	
	/**
	 * 待处理的任务列表
	 * @return
	 */
	public List<TaskLog> queryTaskRunLogList() {
		String sql =null;
		switch (MemCache.DBTYPE) {
		case MYSQL:
			sql = "SELECT seqno,pre_cmd,xmlid,proc_name,pri_level,task_state,start_time,exec_time,end_time,run_freq,agent_code,platform,flowcode,runpara,path,proctype,status_time,retrynum,date_args,proc_date,queue_flag,trigger_flag,time_win,errcode,valid_flag,return_code FROM proc_schedule_log WHERE queue_flag=0 AND valid_flag=0";
			break;
		case ORACLE:
			sql = "SELECT seqno,pre_cmd,xmlid,proc_name,pri_level,task_state,start_time,exec_time,end_time,run_freq,agent_code,platform,flowcode,runpara,path,proctype,status_time,retrynum,date_args,proc_date,queue_flag,trigger_flag,time_win,errcode,valid_flag,return_code FROM proc_schedule_log WHERE queue_flag=0 AND valid_flag=0";
			break;
		default:
		}
		LOG.debug("exec-sql:{}", sql.toString());
		return this.jdbcTemplate.query(sql, new BeanPropertyRowMapper<TaskLog>(TaskLog.class));
	}
	
	/**
	 * 外部接口触发
	 * @return
	 * @throws Exception
	 */
	public List<MetaLog> queryTargetLogList(){
		return this.jdbcTemplate.query("SELECT seqno,proc_name,target,data_time,generate_time FROM proc_schedule_target_log WHERE trigger_flag=0 and data_time is not null", new BeanPropertyRowMapper<MetaLog>(MetaLog.class));
	}
	
	/**
	 * 任务配置刷新
	 * @return
	 */
	public List<TaskConfig> queryNewTaskCfgList(){
		String sql = "SELECT a.xmlid, a.proc_name, b.exec_path path, b.proc_type proctype, agent_code, eff_time, exp_time, trigger_type, run_freq, state, cron_exp, pri_level, platform, redo_num, redo_interval, date_args, pre_cmd, muti_run_flag, team_code, exec_proc, flowcode, max_run_hours, character_set FROM (SELECT xmlid,proc_name,state,team_code FROM proc WHERE STATE = 'PUBLISHED') a, proc_schedule_info b WHERE a.xmlid = b.xmlid";
		LOG.debug("exec-sql:{}", sql.toString());
		return this.jdbcTemplate.query(sql, new BeanPropertyRowMapper<TaskConfig>(TaskConfig.class));
	}
	
	/**
	 * 外部接口重加载,找出后续任务
	 * @return
	 */
	public TaskLog queryTaskRunLog2IntfAgain(String xmlid,String dateArgs) {
		StringBuilder sql = new StringBuilder("SELECT xmlid,proc_name,seqno,task_state,proc_date,date_args,agent_code FROM proc_schedule_log where xmlid ='")
				.append(xmlid).append("' and date_args='").append(dateArgs).append("' and valid_flag = '0'").append(this.getLimitStr());//避免脏数据;
		LOG.debug("exec-sql:{}", sql.toString());
		try {
			return this.jdbcTemplate.queryForObject(sql.toString(), new BeanPropertyRowMapper<TaskLog>(TaskLog.class));
		} catch (EmptyResultDataAccessException ex) {
			return null;
		}
	}
	
	/**
	 * 需要重做当前/重做后续的任务队列
	 * @return
	 */
	public List<TaskLog> queryRedoTaskRunLogList() {
		StringBuilder sql = new StringBuilder("SELECT xmlid,proc_name,seqno,proc_date,date_args,run_freq,trigger_flag FROM proc_schedule_log where task_state=").append(RunStatus.REDO).append(" and queue_flag=0 and valid_flag=0");
		LOG.debug("exec-sql:{}", sql.toString());
		return this.jdbcTemplate.query(sql.toString(), new BeanPropertyRowMapper<TaskLog>(TaskLog.class));
	}

	/**
	 * 查询后续任务信息
	 * @param xmlid
	 * @param dateArgs
	 * @reurn
	 */
	public TaskLog queryfollowUpTask(String xmlid,String dateArgs) {
		StringBuilder sql = new StringBuilder("SELECT xmlid,proc_name,seqno,task_state,proc_date,date_args,agent_code FROM proc_schedule_log where xmlid ='")
				.append(xmlid).append("' and valid_flag = '0'").append(" and date_args='").append(dateArgs).append("' ").append(this.getLimitStr());//避免脏数据
		LOG.debug("exec-sql:{}", sql.toString());
		try {
			return this.jdbcTemplate.queryForObject(sql.toString(), new BeanPropertyRowMapper<TaskLog>(TaskLog.class));
		}catch (EmptyResultDataAccessException ex) {
			return null;
		}	
	}

	/**
	 * 查询任务记录
	 * @param seqNo
	 * @return
	 */
	public TaskLog queryTaskRunLog(String seqNo) {
		String sql = "SELECT seqno,xmlid,proc_name,task_state,run_freq,proc_date,date_args,return_code,exec_time,agent_code from proc_schedule_log where valid_flag =0 and seqno='" + seqNo + "'";
		LOG.debug("exec-sql:{}", sql.toString());
		try {
			return this.jdbcTemplate.queryForObject(sql, new BeanPropertyRowMapper<TaskLog>(TaskLog.class));
		} catch (EmptyResultDataAccessException ex) {
			return null;
		}
	}
	
	/**
	 * 查询任务记录
	 * @param seqNo
	 * @return
	 */
	public TaskLog queryRunningTaskRunLog(String seqNo) {
		String sql = "SELECT seqno,xmlid,proc_name,task_state,run_freq,proc_date,date_args,return_code,exec_time,agent_code from proc_schedule_log where task_state='5' and valid_flag =0 and seqno='" + seqNo + "'";
		LOG.debug("exec-sql:{}", sql.toString());
		try {
			return this.jdbcTemplate.queryForObject(sql, new BeanPropertyRowMapper<TaskLog>(TaskLog.class));
		} catch (EmptyResultDataAccessException ex) {
			return null;
		}
	}
	
	/**
	 * 查询任务记录
	 * @param seqNo
	 * @return
	 */
	public TaskLog queryRunningTaskRunLog(String xmlid,String dateArgs) {
		String sql = "SELECT seqno,xmlid,proc_name,task_state,run_freq,proc_date,date_args,return_code,exec_time,agent_code from proc_schedule_log where task_state='5' and valid_flag =0 and xmlid='" + xmlid + "' and date_args='"+dateArgs+"'";
		LOG.debug("exec-sql:{}", sql.toString());
		try {
			return this.jdbcTemplate.queryForObject(sql, new BeanPropertyRowMapper<TaskLog>(TaskLog.class));
		} catch (EmptyResultDataAccessException ex) {
			return null;
		}
	}
	/**
	 * 刷新任务配置缓存
	 * @return
	 */
	public List<TaskConfig> queryTaskConfigList() {
		String now = TimeUtils.getCurrentTime("yyyy-MM-dd");
		StringBuilder sql = new StringBuilder("SELECT * FROM (SELECT a.xmlid, a.proc_name, b.exec_path path, b.proc_type proctype, agent_code, eff_time, exp_time, trigger_type, run_freq, state, cron_exp, pri_level, platform, redo_num, redo_interval, date_args, pre_cmd, muti_run_flag, team_code, exec_proc, max_run_hours, character_set FROM proc a, proc_schedule_info b WHERE a.xmlid = b.xmlid) c WHERE ");
		sql.append("STATE='VALID' and eff_time<='").append(now).append("' and exp_time>'").append(now).append("'");
		LOG.debug("exec-sql:{}", sql.toString());
		return this.jdbcTemplate.query(sql.toString(), new BeanPropertyRowMapper<TaskConfig>(TaskConfig.class));
	}
	
	/**
	 * 查询任务配置关系
	 * @param xmlid
	 * @return
	 */
	public List<TransdatamapDesignBean> queryTransdatamapDesign(String xmlid) {
		StringBuilder sql = new StringBuilder("SELECT transname,source,sourcetype ,sourcefreq,target,targettype ,targetfreq  FROM transdatamap_design");
		if (StringUtils.isNotEmpty(xmlid)) {
			sql.append(" where transname='").append(xmlid).append("'");
		}
		LOG.debug("exec-sql:{}", sql.toString());
		return this.jdbcTemplate.query(sql.toString(), new BeanPropertyRowMapper<TransdatamapDesignBean>(TransdatamapDesignBean.class));
	}
	
	/**
	 * 查询程序依赖条件
	 * @param seqno
	 * @return
	 * @throws Exception
	 */
	public List<SourceLog> querySourceLogList(String seqno) throws Exception {
		StringBuilder sql = new StringBuilder("SELECT seqno,source,source_type,data_time FROM proc_schedule_source_log where seqno='").append(seqno).append("' and check_flag=0");
		LOG.debug("exec-sql:{}", sql.toString());
		return this.jdbcTemplate.query(sql.toString(), new BeanPropertyRowMapper<SourceLog>(SourceLog.class));
	}
	
	/**
	 * 查询需要刷新的任务运行信息
	 * 状态 1,2,3,-7(计划任务在刷新任务时候,直接变为创建成功,以便检测条件是否到达)
	 * @param xmlid
	 * @return
	 * @throws Exception
	 */
	public List<TaskLog> getNeedrefurbishOldTaskLog(String xmlid) throws Exception {
		StringBuilder sql = new StringBuilder("SELECT seqno,date_args FROM proc_schedule_log where xmlid='").append(xmlid).append("' and queue_flag =0 and valid_flag=0")
				.append(" and task_state in (1,2,3,-7)");
		LOG.debug("exec-sql:{}", sql.toString());
		return this.jdbcTemplate.query(sql.toString(), new BeanPropertyRowMapper<TaskLog>(TaskLog.class));
	}
	
	public int checkExist(String tableName, String[] fieldNames, Object[] values) throws Exception {
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
	
	/**
	 * 创建任务判断
	 * @param xmlid
	 * @param dateArgs
	 * @return
	 * @throws Exception
	 */
	public int isTaskExist(String xmlid, String dateArgs) throws Exception {
		try {
			return jdbcTemplate.queryForObject("select 1 from proc_schedule_log where xmlid=? and date_args=? and valid_flag=0 and task_state>-7 " + getLimitStr(), Integer.class, xmlid,dateArgs);			
		} catch (EmptyResultDataAccessException ex) {
			return 0;
		}
	}
	
	/**
	 * 创建未触发任务信息,判断是否已经存在
	 * @param xmlid
	 * @param dateArgs
	 * @return
	 * @throws Exception
	 */
	public int isTaskExist2Standby(String xmlid, String dateArgs) throws Exception {
		try {
			return jdbcTemplate.queryForObject("select 1 from proc_schedule_log where xmlid=? and date_args=? and valid_flag=0 " + getLimitStr(), Integer.class, xmlid,dateArgs);			
		} catch (EmptyResultDataAccessException ex) {
			return 0;
		}
	}
	
	public int checkExistLastNon(String tableName, String[] fieldNames, Object[] values) throws Exception {
		String sql = "select 1 num from " + tableName + " where ";
		for (int i = 0; i < fieldNames.length; i++) {
			if (i != fieldNames.length - 1) {
				sql += fieldNames[i] + "=? and ";
			} else {
				sql += fieldNames[i] + "<>? " + getLimitStr();
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
	
	
	/**
	 * 通过xmlid查询运行参数
	 * @param xmlid
	 * @return
	 */
	public List<RunPara> queryRunpara(String xmlid) {
		return this.jdbcTemplate.query("SELECT run_para,run_para_value FROM proc_schedule_runpara where xmlid='" + xmlid+ "' order by orderid asc",
				new BeanPropertyRowMapper<RunPara>(RunPara.class));
	}
	
	public List<TaskGlobalVal> queryGlobalVal() {
		return this.jdbcTemplate.query("select var_name,var_type,var_value from schedule_task_global_val",new BeanPropertyRowMapper<TaskGlobalVal>(TaskGlobalVal.class));
	}
	
	/**
	 * 初始化刷新agent配置
	 * @return
	 */
	public List<AgentIps> queryAgentIps() {
		String sql = "SELECT agent_name as agent_code,ips,platform FROM aietl_agentnode where task_type = 'TASK'";
		return this.jdbcTemplate.query(sql, new BeanPropertyRowMapper<AgentIps>(AgentIps.class));
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
	 * 通用检测是否存在记录
	 * @param sql
	 * @return
	 * @throws Exception
	 */
	public int checkExist(String sql) throws Exception {
		try {
			LOG.debug("exec-sql:{}", sql.toString());
			jdbcTemplate.queryForObject(sql, Integer.class);
			return 1;
		} catch (EmptyResultDataAccessException ex) {
			return 0;
		}
	}
}
