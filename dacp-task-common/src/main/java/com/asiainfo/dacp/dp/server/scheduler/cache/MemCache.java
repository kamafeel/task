package com.asiainfo.dacp.dp.server.scheduler.cache;

import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.asiainfo.dacp.dp.server.scheduler.bean.AgentIps;
import com.asiainfo.dacp.dp.server.scheduler.bean.SourceObj;
import com.asiainfo.dacp.dp.server.scheduler.bean.TableInfo;
import com.asiainfo.dacp.dp.server.scheduler.bean.TargetObj;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskConfig;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskLog;
import com.asiainfo.dacp.dp.server.scheduler.type.DBType;

public class MemCache {
	public static DBType DBTYPE;
	/** 程序基本配置 */
	public final static Map<String, TaskConfig> PROC_MAP = new ConcurrentHashMap<String, TaskConfig>();
	/** 程序触发信息 */
	public final static Map<String, Map<String,TargetObj>> TARGET_MAP = new ConcurrentHashMap<String, Map<String,TargetObj>>();
	/** 程序依赖信息 */
	public final static Map<String, Map<String,SourceObj>> SOURCE_MAP = new ConcurrentHashMap<String, Map<String,SourceObj>>();
	/** 执行模型检测错误缓存 */
	public final static Map<String, Integer> TASK_WAIT_REASON= new ConcurrentHashMap<String, Integer>();
	/** 任务运行缓存 yyyy-MM-dd */
	public final static Map<String, String> RUN_TASK = new ConcurrentHashMap<String, String>();
	/** 任务人工停止批次 */
	public final static Map<String, String> MANUAL_STOP = new ConcurrentHashMap<String, String>();
	/** agent并发情况缓存 ZK监听 */
	public final static Map<String, AgentIps> AGENT_IPS_MAP = new ConcurrentHashMap<String, AgentIps>();
	/** agent并发情况缓存 内存模式 */
	public final static Map<String, Integer> AGENT_IPS_MAP_MC = new ConcurrentHashMap<String, Integer>();
	/** agent并发情况缓存 内存 */
	public final static Map<String, HashSet<String>> AGENT_RUN_MAP = new ConcurrentHashMap<String, HashSet<String>>();
	/** server运行态缓存 */
	//public final static Map<String, String> SERVER_MAP = new ConcurrentHashMap<String, String>();
	/** 批量数据操作缓存 */
	public static final Queue<String> DB_EXECUTE_QUEUE = new ConcurrentLinkedQueue<String>();
	/** 批量数据操作字典类 */
	public static final Map<String,String> DB_EXECUTE_QUEUE_DICTIONARY = new ConcurrentHashMap<String,String>();
	/** 批量数据操作监测 */
	public static Map<String, Long> DB_EXECUTE_QUEUE_MONITOR = new ConcurrentHashMap<String, Long>();	
	/** 重做任务从错误号开始 */
	public final static Map<String, Integer> REDO_FROM_ERRORCODE = new ConcurrentHashMap<String, Integer>();	
	/** 程序依赖事件缓存 */
	public final static Map<String, Map<String,Integer>> SOURCE_EVENT = new ConcurrentHashMap<String, Map<String,Integer>>();
	/** 执行模式事件缓存 */
	public final static Map<String, Map<String,Integer>> RUNMODE_EVENT = new ConcurrentHashMap<String, Map<String,Integer>>();	
	/** 各地个性化任务处理 */
	public static final Queue<TaskLog> TASK_STYLE_QUEUE = new ConcurrentLinkedQueue<TaskLog>();
	/** 暂停任务缓存 */
	public final static Map<String, Integer> PROC_PAUSE_MAP = new ConcurrentHashMap<String, Integer>();	
	/** 表,接口等信息缓存 */
	public final static Map<String, TableInfo> EVENT_ATTRIBUTE = new ConcurrentHashMap<String, TableInfo>();
	/** 任务成功缓存 */
	//public static Map<String, Integer> EVENT_SUC_FIFO = null;
}
