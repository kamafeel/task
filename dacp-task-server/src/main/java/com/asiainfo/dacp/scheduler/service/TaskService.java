package com.asiainfo.dacp.scheduler.service;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.asiainfo.dacp.dp.message.MapKeys;
import com.asiainfo.dacp.dp.common.RunStatus;
import com.asiainfo.dacp.dp.common.RunStatus.IsValid;
import com.asiainfo.dacp.dp.message.DpMessage;
import com.asiainfo.dacp.dp.message.DpSender;
import com.asiainfo.dacp.dp.server.scheduler.bean.FollowUpTask;
import com.asiainfo.dacp.dp.server.scheduler.bean.MetaLog;
import com.asiainfo.dacp.dp.server.scheduler.bean.SourceLog;
import com.asiainfo.dacp.dp.server.scheduler.bean.SourceObj;
import com.asiainfo.dacp.dp.server.scheduler.bean.TableInfo;
import com.asiainfo.dacp.dp.server.scheduler.bean.TargetObj;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskConfig;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskLog;
import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbSearch;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbStorage;
import com.asiainfo.dacp.dp.server.scheduler.type.DataFreq;
import com.asiainfo.dacp.dp.message.MsgType;
import com.asiainfo.dacp.dp.server.scheduler.type.ObjType;
import com.asiainfo.dacp.dp.server.scheduler.type.ProcStatus;
import com.asiainfo.dacp.dp.server.scheduler.type.RunFreq;
import com.asiainfo.dacp.dp.server.scheduler.type.Type;
import com.asiainfo.dacp.dp.server.scheduler.utils.UUIDUtils;
import com.asiainfo.dacp.dp.tools.TimeUtils;
import com.asiainfo.dacp.scheduler.command.CmdLineBuilder;
import com.asiainfo.dacp.scheduler.quartz.JobScheduleController;
import com.asiainfo.dacp.scheduler.quartz.QuartzJob;
import com.google.gson.Gson;

/**
 * 任务基础服务
 * @author zhangqi
 *
 */
@Service
public class TaskService {
	
	private static Logger LOG = LoggerFactory.getLogger(TaskService.class);
	
	@Autowired
	private DpSender dpSender;
	@Autowired
	private TaskEventRdbSearch search;
	@Autowired
	private TaskEventRdbStorage storage;
	@Autowired
	private TaskCacheService tcs;
	@Autowired
	private CmdLineBuilder cmdLineBuilder;
	
	@Autowired
	private JobScheduleController jobScheduleController;
	
	@Value("${mq.request.queue.name}")
	private String request_queue_name;
	@Value("${mq.receiveQueue}")
	private String response_queue_name;
	@Value("${mq.stuck.activate}")
	private int mqStuck;
	@Value("${mq.stuck.timeout}")
	private int mqStuckTimeout;
	@Value("${rule.trigger.timeType}")
	private boolean isTriggerTimeType;
	@Value("${rule.intf.reload}")
	private boolean isIntfReload;
	@Value("${rule.newConfig.createTask}")
	private boolean isNewConfigCreateTask;
	
	@Value("${runlog2db}")
	private boolean runLog2db;
	@Value("${paas.version}")
	private String paasVersion;

	public boolean isRunLog2db() {
		return runLog2db;
	}

	public void setRunLog2db(boolean runLog2db) {
		this.runLog2db = runLog2db;
	}

	public boolean isTriggerTimeType() {
		return isTriggerTimeType;
	}

	public void setTriggerTimeType(boolean isTriggerTimeType) {
		this.isTriggerTimeType = isTriggerTimeType;
	}

	public DpSender getDpSender() {
		return dpSender;
	}

	public void setDpSender(DpSender dpSender) {
		this.dpSender = dpSender;
	}

	public String getRequest_queue_name() {
		return request_queue_name;
	}

	public void setRequest_queue_name(String request_queue_name) {
		this.request_queue_name = request_queue_name;
	}

	public String getResponse_queue_name() {
		return response_queue_name;
	}

	public void setResponse_queue_name(String response_queue_name) {
		this.response_queue_name = response_queue_name;
	}

	public int getMqStuck() {
		return mqStuck;
	}

	public void setMqStuck(int mqStuck) {
		this.mqStuck = mqStuck;
	}

	public int getMqStuckTimeout() {
		return mqStuckTimeout;
	}

	public void setMqStuckTimeout(int mqStuckTimeout) {
		this.mqStuckTimeout = mqStuckTimeout;
	}
	
	public void refurbishTableInfo(){
		List<TableInfo> l = search.queryTableInfo(paasVersion);
		MemCache.EVENT_ATTRIBUTE.clear();
		for(TableInfo ti : l){
			MemCache.EVENT_ATTRIBUTE.put(ti.getId(), ti);
		}
	}
	
	/**
	 * 分发消息(重庆使用)
	 * @param metaList
	 */
	public void pushMessage(List<MetaLog> metaList) {
		if (metaList.isEmpty()) {
			return;
		}
		DpMessage msgContent = new DpMessage();
		msgContent.setClassMethod("updateDBStatus");
		msgContent.setClassUrl("com.asiainfo.proc.DataTransTask");
		msgContent.setMsgType("TRANS");
		msgContent.setSourceQueue("transServer");
		for (MetaLog log : metaList) {
			Map<String, String> map = new HashMap<String, String>();
			map.put("XMLID", log.getTarget());
			map.put("op_time", log.getDataTime());
			msgContent.addBody(map);
		}
		if (this.getDpSender().pushMessage("dacp-trans-exchange", msgContent,1000*10)) {//本地个性化配置,不需要参数化
			LOG.info("push {}  message to trans-server success [{}]", metaList.size(),new Gson().toJson(msgContent));
		} else {
			LOG.info("push {}  message to trans-server fail", Integer.valueOf(metaList.size()));
		}
	}
	
	/**
	 * 消息分发(重庆专用)
	 * @param dataMap
	 */
	public void resetDataTime(Map<String, String> dataMap) {
		String[] a = null;
		String tmp = null;
		String target = dataMap.get("target");
		String dataTime = dataMap.get("dataTime");
		String interNo = dataMap.get("interNo");
		if (StringUtils.isNotEmpty(target) && StringUtils.isNotEmpty(dataTime) && StringUtils.isEmpty(interNo)) {
			if (MemCache.TARGET_MAP.containsKey(target)) {
				Set<Map.Entry<String, TargetObj>> set = MemCache.TARGET_MAP.get(target).entrySet();
				Iterator<Map.Entry<String, TargetObj>> it = set.iterator();
				while (it.hasNext()) {
					Map.Entry<String, TargetObj> item = it.next();
					a = item.getValue().getSourcefreq().split("-");
					tmp = dataTime.substring(6, 8);
					if (a.length >= 1 && DataFreq.M.name().equals(a[0]) && tmp.equals("01")) {
						dataTime = dataTime.substring(0, 6);
						dataMap.put("dataTime", dataTime);
					}
					break;// 只做一条记录
				}
			}
		}
	}
	
	/**
	 * 转换MetaLog(重庆专用)
	 * @param dataMap
	 * @return
	 * @throws Exception 
	 */
	public MetaLog convertToMetaLog(Map<String, String> dataMap) throws Exception {
		if (dataMap == null) {
			return null;
		}
		MetaLog targetLog = new MetaLog();
		targetLog.setSeqno(UUIDUtils.getUUID());
		String target = dataMap.get("target");
		String procDate = dataMap.get("procDate");
		String dataTime = dataMap.get("dataTime");
		String interNo = dataMap.get("interNo");
		if (StringUtils.isNotEmpty(target) && StringUtils.isNotEmpty(dataTime)) {
			targetLog.setTarget(target);
			targetLog.setProcDate(procDate);
			targetLog.setProcName(interNo);
			targetLog.setDataTime(dataTime);
			targetLog.setNeedDqCheck(0);
			targetLog.setDqCheckRes(1);
			targetLog.setDateArgs(TimeUtils.convertDataTime2CQ(dataTime));
			targetLog.setGenerateTime(TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"));
			if (StringUtils.equals(dataTime, DataFreq.N.name())) {
				targetLog.setTriggerFlag(1);
			} else {
				targetLog.setTriggerFlag(0);
			}
		}
		return targetLog;
	}
	
	/**
	 * 发送mq消息
	 * @param tl
	 * @return
	 * @throws Exception
	 */
	public boolean sendProcToAgent(TaskLog tl) throws Exception {
		StringBuilder queueName = new StringBuilder(tl.getAgentCode()).append("_").append(this.getRequest_queue_name());
		DpMessage msgObj = new DpMessage();
		msgObj.setMsgId(tl.getSeqno());
		msgObj.setMsgType(MsgType.taskTypeFunc.name());// 所有类型都当做执行脚本执行
		Map<String, String> map = cmdLineBuilder.buildCmdLine(tl);
		msgObj.setSourceQueue(this.getResponse_queue_name());
		msgObj.addBody(map);
		msgObj.setDateArgs(tl.getDateArgs());
		if (this.getDpSender().sendMessage(queueName.toString(), msgObj,1000*10)) {
			LOG.info("task[{}] send mq[{}] success ,message:{}", 
					new String[]{tl.getTaskId(), queueName.toString(),map.get(MapKeys.CMD_PARA)});
			return true;
		} else {
			LOG.info("task[{}] send mq[{}] fail,message:{}", 
					new String[]{tl.getTaskId(), queueName.toString(),map.get(MapKeys.CMD_PARA)});
//			MemCache.RUN_TASK.remove(tl.getXmlid(), tl.getDateArgs());
			TaskCacheService.removeRunTaskTemp(tl.getXmlid(), tl.getDateArgs());
			return false;
		}
	}

	/**
	 * 依赖条件检测
	 * @param tl
	 */
	public void CheckDpend(TaskLog tl){
		try {
			if(!MemCache.SOURCE_MAP.containsKey(tl.getXmlid())){
				LOG.info("task[{}] hasn‘t depend,will be next",tl.getTaskId());
				storage.updateTaskState(tl.getSeqno(), RunStatus.CHECK_DEPEND_SUCCESS);
				return;
			}
			Map<String, Object> dataMap = new HashMap<String, Object>();
			Map<String,Object> condition = new HashMap<String, Object>();
			String seKey = tl.getXmlid()+"@"+tl.getDateArgs();
			boolean checkPass = false;
			if(!MemCache.SOURCE_EVENT.containsKey(seKey)){
				MemCache.SOURCE_EVENT.put(seKey, new ConcurrentHashMap<String,Integer>());
				List<SourceLog> slList = search.querySourceLogList(tl.getSeqno());
				int allSrouce = slList.size();
				int ckRes=0;
				Object[] values = null;
				String[] targetFileds = new String[] { "target", "data_time"};
				//need create db index
				String[] procFileds = new String[] { "xmlid", "date_args", "task_state","valid_flag" };		
				for(SourceLog sl : slList){
					ckRes = 0;
					switch (ObjType.valueOf(sl.getSourceType())) {
					case DATA:
						values = new Object[] { sl.getSource(), sl.getDataTime() };
						ckRes = search.checkExist("proc_schedule_meta_log", targetFileds, values);
						break;
					case INTER:
						values = new Object[] { sl.getSource(), sl.getDataTime() };
						ckRes = search.checkExist("proc_schedule_meta_log", targetFileds, values);
						break;
					case EVENT:
						values = new Object[] { sl.getSource(), sl.getDataTime() };
						ckRes = search.checkExist("proc_schedule_meta_log", targetFileds, values);
						break;
					case PROC:
						values = new Object[] { sl.getSource(), sl.getDataTime(), RunStatus.PROC_RUN_SUCCESS, IsValid.VALID_FLAG };
						ckRes = search.checkExist("proc_schedule_log", procFileds, values);
						break;
					default:
						ckRes = 0;
						break;
					}
					
					if(ckRes>0){
						dataMap.clear();
						dataMap.put("check_flag", 1);
						dataMap.put("check_time", TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"));
						condition.clear();
						//调整顺序,提高SQL执行效率
						condition.put("source", sl.getSource());
						condition.put("seqno", sl.getSeqno());					
						storage.update2Transaction("proc_schedule_source_log", dataMap, condition);
						allSrouce--;
					}else{
						MemCache.SOURCE_EVENT.get(seKey).put(sl.getSource() + "@" + sl.getDataTime(), 0);
					}
				}
				if(allSrouce == 0){
					checkPass = true;
				}
			}else{
				Set<Map.Entry<String, Integer>> set = MemCache.SOURCE_EVENT.get(seKey).entrySet();
				Iterator<Map.Entry<String, Integer>> it = set.iterator();
				while (it.hasNext()) {
					Map.Entry<String, Integer> item = it.next();
					String sourceAndDateArgs = item.getKey();
					//if(item.getValue() == 1 || MemCache.EVENT_SUC_FIFO.containsKey(sourceAndDateArgs)){
					if(item.getValue() == 1){
						MemCache.SOURCE_EVENT.get(seKey).remove(sourceAndDateArgs);
						dataMap.clear();
						dataMap.put("check_flag", 1);
						dataMap.put("check_time", TimeUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss"));
						condition.clear();
						//调整顺序,提高SQL执行效率
						condition.put("source", sourceAndDateArgs.split("@")[0]);
						condition.put("seqno", tl.getSeqno());
						storage.update2Transaction("proc_schedule_source_log", dataMap, condition);
					}
				}
				if(MemCache.SOURCE_EVENT.get(seKey).isEmpty()){
					checkPass = true;
				}
			}
			
			if(checkPass){
				LOG.info("task[{}] check depend success",tl.getTaskId());
				storage.updateTaskState(tl.getSeqno(), RunStatus.CHECK_DEPEND_SUCCESS);
				MemCache.SOURCE_EVENT.remove(tl.getXmlid()+"@"+tl.getDateArgs());
			}
			
		} catch (Exception e) {
			LOG.error("hard error,{}",tl== null ? "can't found taskLog" : tl.getTaskId(),e);
		}
	}
	
	/**
	 * 剔除非月末,日末的触发,非小时的触发
	 * @param mt
	 * @param tl
	 * @throws Exception
	 */
	public void nonLastTrigger(Map<String,TargetObj> mt,String dateArgs,boolean isData) throws Exception{
		Set<Map.Entry<String, TargetObj>> set = mt.entrySet();
		Iterator<Map.Entry<String, TargetObj>> it = set.iterator();
		while (it.hasNext()) {
			Map.Entry<String, TargetObj> item = it.next();
			TargetObj to = item.getValue();
			String sf = to.getSourcefreq();
			if(sf.equals(DataFreq.N.name())){
				mt.remove(item.getKey());
				continue;
			}
			if(sf.contains("ML") || sf.contains("DL")){
				if(!TimeUtils.isLast(sf.contains("ML")? "day":"hour", dateArgs ,isData)){
					mt.remove(item.getKey());
				}
			}
			if(sf.contains("DH") || sf.contains("DD")){
				if(!TimeUtils.isDesignated(sf.contains("DD")? "day":"hour", sf.split("#")[1],dateArgs ,isData)){
					mt.remove(item.getKey());
				}
			}
		}
	}
	
	
	/**
	 * 剔除后续为时间触发的任务
	 * @param mt
	 */
	public void clearTriggerTimeTask(Map<String,TargetObj> mt){
		//剔除时间类型任务
		if(!this.isTriggerTimeType()){
			Set<Map.Entry<String, TargetObj>> set = mt.entrySet();
			Iterator<Map.Entry<String, TargetObj>> it = set.iterator();
			while (it.hasNext()) {
				Map.Entry<String, TargetObj> item = it.next();
				String targetXmlid = item.getKey();					
				if(MemCache.PROC_MAP.containsKey(targetXmlid) 
						&& MemCache.PROC_MAP.get(targetXmlid).getTriggerType().intValue() == Type.DRIVE_TYPE.TIME_TRIGGER.ordinal()){
					mt.remove(targetXmlid);
				}
			}
		}
	}
	
	
	/**
	 * @throws Exception 
	 * 外部接口触发
	 * @throws  
	 */
	public void outIntfDataTrigger() throws Exception{
		List<MetaLog> mll = search.queryTargetLogList();
		if(mll == null || mll.isEmpty()){
			return;
		}else{
			LOG.info("start to trigger outIntfData [{}]",mll.size());
		}
		String[] targetFileds = new String[] { "target", "data_time" };
		for(MetaLog ml : mll){
			String target = ml.getTarget();
			String dataTime = ml.getDataTime();
			try {
				if(search.checkExist("proc_schedule_meta_log", targetFileds, new Object[] {target,dataTime}) == 0){
					LOG.info("data[{}] is first trigger", ml.getDataId());
					//获取数据后续的程序					
					if(!MemCache.TARGET_MAP.containsKey(target)){
						continue;
					}
					Map<String,TargetObj> mtdata = new ConcurrentHashMap<String,TargetObj>();
					mtdata.putAll(MemCache.TARGET_MAP.get(target));
					this.nonLastTrigger(mtdata, dataTime, true);
					this.refurbishSoureEvent2IntfData(target, dataTime);					
					//剔除时间类型任务
					this.clearTriggerTimeTask(mtdata);

					LOG.info("data[{}] has [{}] task", ml.getDataId(),mtdata==null ? 0 : mtdata.size());
					if(mtdata == null || mtdata.isEmpty()){
						continue;
					}					
					Set<Map.Entry<String, TargetObj>> setData = mtdata.entrySet();
					Iterator<Map.Entry<String, TargetObj>> itdata = setData.iterator();
					while (itdata.hasNext()) {
						Map.Entry<String, TargetObj> itemData = itdata.next();
						TargetObj objData = itemData.getValue();					
						switch (ObjType.valueOf(objData.getTargettype())) {
						case PROC:
							if(MemCache.PROC_MAP.get(objData.getTarget())!=null) {
								TaskLog tl = createTaskRunInfo(MemCache.PROC_MAP.get(objData.getTarget()), null,
										TimeUtils.getIntfDataTargetProcDateArgs(objData.getSourcefreq(),ml.getDataTime()));						
								LOG.info("data[{},{}] create next task:[{}]", new String[]{objData.getSource(),dataTime, tl == null ? "TaskExist" : tl.getTaskId()});
							}
							break;
						default:
							LOG.error("data[{},{}] can't create next data:[{}]", new String[]{objData.getSource(),dataTime,objData.getTarget()}); 
							break;
						}
					}			
				}else{
					LOG.info("data[{}] is again trigger,system will be redoTask [{}]", ml.getDataId(),isIntfReload);
					if(!isIntfReload){
						continue;
					}
					List<FollowUpTask> l = this.redoFlowUpTasks(target, dataTime);
					if(l == null || l.isEmpty()){
						continue;
					}
					for(FollowUpTask fut : l){
						TaskLog tl = search.queryTaskRunLog2IntfAgain(fut.getXmlid(),fut.getDateArgs());
						if(tl != null){
							switch (tl.getTaskState()) {
							case RunStatus.PROC_RUN_SUCCESS:
							case RunStatus.PROC_RUN_FAIL:// 有执行结果的记录保留日志，失效任务
								storage.setTaskInvalid(tl.getSeqno()); // 失效任务
								TaskLog tlredo = this.createTaskRunInfo(MemCache.PROC_MAP.get(tl.getXmlid()),null,tl.getDateArgs());
								LOG.info("old task[{}] tobe dead,new task[{}] tobe create because data[{}] reload",
										new String[]{tl.getTaskId(),tlredo == null ? "TaskExist" : tlredo.getTaskId(),ml.getTarget()});
								break;
							case RunStatus.SEND_TO_MQ:
							case RunStatus.PROC_RUNNING:// 正在运行的任务，直接停止任务,并且重做
								storage.setTaskInvalid(tl.getSeqno());// 失效任务
								this.killProcess(tl);
								TaskLog tlreRun = this.createTaskRunInfo(MemCache.PROC_MAP.get(tl.getXmlid()),null,tl.getDateArgs());
								LOG.info("old task[{}] will be kill,new task[{}] will be rerun because data[{}] reload",
										new String[]{tl.getTaskId(),tlreRun == null ? "TaskExist" : tlreRun.getTaskId(),ml.getTarget()});
								break;
							default:
								break;
							}
						}
					}
				}
			} catch (Exception e) {
				LOG.error("data[{}] trigger is fail", ml.getDataId(),e);
			} finally {
				//外界数据直接输出到metaLog,不做额外处理
				storage.insertMetaLog(ml);
				storage.updateTargetLog(ml);
			}			
		}
		if(!mll.isEmpty()){
			LOG.info("trigger outIntfData [{}] is finish",mll.size());
		}		
	}
	
	/**
	 * 刷新任务依赖事件
	 * @param mt
	 * @throws Exception 
	 */
	private void refurbishSoureEvent2IntfData(String target,String dataTime) throws Exception{
		String sucKey = target+ "@" + dataTime;
		Set<Entry<String, Map<String, Integer>>> set = MemCache.SOURCE_EVENT.entrySet();
		Iterator<Entry<String, Map<String, Integer>>> it = set.iterator();
		while (it.hasNext()) {	
			Entry<String, Map<String, Integer>> item = it.next();
			String soureKey = item.getKey();
			Set<Entry<String, Integer>> set_ = item.getValue().entrySet();
			Iterator<Entry<String, Integer>> it_ = set_.iterator();
			while (it_.hasNext()) {
				if(it_.next().getKey().equals(sucKey)){
					MemCache.SOURCE_EVENT.get(soureKey).put(sucKey, 1);					
				}
			}
		}
	}
	
	
	
	/**
	 * 发布事件到Meta表,事件由程序输出,非外界数据
	 * @param tl
	 * @throws Exception 
	 */
	public MetaLog push2Meta(TaskLog tl,TargetObj obj) throws Exception{
		MetaLog metaLog = new MetaLog();
		metaLog.setSeqno(tl.getSeqno());
		metaLog.setProcName(tl.getXmlid());
		metaLog.setTarget(obj.getTarget());
		//输出表时间 格式
		metaLog.setDataTime(TimeUtils.getProcTargetDataDataTime(obj.getTargetfreq(),tl.getDateArgs()));
		metaLog.setGenerateTime(TimeUtils.date2String(new Date(), "yyyy-MM-dd HH:mm:ss"));
		storage.insertMetaLog(metaLog);
		return metaLog;
	}
	
	/**
	 * 发布事件到Meta表,事件由程序输出,非外界数据(重庆使用)
	 * @param tl
	 * @throws Exception 
	 */
	public MetaLog getMeta(TaskLog tl,TargetObj obj) throws Exception{
		MetaLog metaLog = new MetaLog();
		metaLog.setSeqno(tl.getSeqno());
		metaLog.setProcName(tl.getXmlid());
		metaLog.setTarget(obj.getTarget());
		//输出表时间 格式
		metaLog.setDataTime(TimeUtils.getProcTargetDataDataTime(obj.getTargetfreq(),tl.getDateArgs()));
		metaLog.setGenerateTime(TimeUtils.date2String(new Date(), "yyyy-MM-dd HH:mm:ss"));
		return metaLog;
	}
	
	/**
	 * Meta表删除事件
	 * @param tl
	 * @throws Exception 
	 */
	public void delete2Meta(String target,String dataTime) throws Exception{
		MetaLog metaLog = new MetaLog();
		metaLog.setTarget(target);
		metaLog.setDataTime(dataTime);
		storage.deleteMetaLog(metaLog);
		LOG.info("data [{},{}] tobe delete from MetaLog",target,dataTime);
	}
	
	/**
	 * 刷新任务配置
	 * @throws Exception 
	 * @throws ParseException 
	 * @throws NumberFormatException 
	 */
	public void refurbishProcConfig(){
		List<TaskConfig> newtcs = search.queryNewTaskCfgList();
		if(!newtcs.isEmpty()){
			LOG.info("start to refurbish [{}] task config",newtcs.size());
		}else{
			return;
		}
		
		for(TaskConfig tc : newtcs){
			if(!MemCache.PROC_MAP.containsKey(tc.getXmlid()) && this.isTaskExpTimeInValid(tc)){
				MemCache.PROC_MAP.put(tc.getXmlid(), tc);
			}
		}
		
		for(TaskConfig tc : newtcs){
			//更新配置
			String procStatus = ProcStatus.INVALID.name();
			try {				
				if(MemCache.PROC_MAP.containsKey(tc.getXmlid())){				
					if(this.isTaskExpTimeInValid(tc)){
						this.refurbishTaskCfgInTransdatamapDesign(tc.getXmlid());
						this.refurbishProcConfig2Old(tc);
						LOG.info("update config of task[{}] success,to refurbish",tc.getProcName());
						procStatus = ProcStatus.VALID.name();
					}else{
						this.clearTaskCfgInTransdatamapDesign(tc.getXmlid());
						this.refurbishProcConfig2Dead(tc);
						LOG.info("update config of task[{}] success,to dead",tc.getProcName());
					}
				//全新任务
				} else{
					if(this.isTaskExpTimeInValid(tc)){
						this.refurbishProcConfig2New(tc);
						LOG.info("update config of task[{}] success,to new",tc.getProcName());
						procStatus = ProcStatus.VALID.name();
					} 
				}
			} catch (Exception e) {
				LOG.error("update config of task[{}] fail",tc.getProcName(),e);
			} finally {
				storage.updateProcState(tc.getXmlid(),procStatus);
			}
		}
		if(!newtcs.isEmpty()){
			LOG.info("refurbish [{}] task config is finish",newtcs.size());
		}		
	}
	
	/**
	 * 全新任务配置刷新
	 * @param config
	 * @throws NumberFormatException
	 * @throws ParseException
	 * @throws Exception
	 */
	public void refurbishProcConfig2New(TaskConfig config) throws NumberFormatException, ParseException, Exception {
		String xmlid = config.getXmlid();
		MemCache.PROC_MAP.put(xmlid, config);
		tcs.refurbishTransdatamapDesign(xmlid);
				
		//时间触发刷新
		if(config.getTriggerType() == Type.DRIVE_TYPE.TIME_TRIGGER.ordinal()){
//			jobScheduleController.rescheduleJob(xmlid, config.getCronExp());
			LOG.info("rescheduleJob[{}] cron [{}] ", xmlid, config.getCronExp());
			jobScheduleController.scheduleJob(xmlid, config.getCronExp(), QuartzJob.class);
		}
		
		boolean isdirtyData = false;
		//只处理当前有效的任务运行日志,已经无效的暂不处理(按道理新任务不应该存在记录,但是避免脏数据的影响)
		for(TaskLog tl : search.getNeedrefurbishOldTaskLog(config.getXmlid())){
			isdirtyData = true;
			storage.deleteTaskLog(tl.getSeqno());
			//重新创建起来
			this.createTaskRunInfo(config, null, tl.getDateArgs());
		}
		
		if(!isdirtyData && isNewConfigCreateTask){
			TaskLog tl = null;
			if(config.getRunFreq().equals(RunFreq.day.name())){
				tl = this.createStandbyTaskRunInfo(config, null);
			}else if(config.getRunFreq().equals(RunFreq.month.name())){
				tl = this.createStandbyTaskRunInfo(config, TimeUtils.getInEarlyOfMon());
			}
			this.isSourceReady(tl);
		}
	}
	
	/**
	 * 任务条件在任务创建之前已经达到,任务需要切换状态为任务成功
	 * @param tl
	 * @return
	 * @throws Exception 
	 */
	private void isSourceReady(TaskLog tl) throws Exception{
		if(tl == null){
			return;
		}
		Map<String,SourceObj> map = MemCache.SOURCE_MAP.get(tl.getXmlid());
		if (map != null && !map.isEmpty()){
			Set<Map.Entry<String, SourceObj>> set = map.entrySet();
			Iterator<Map.Entry<String, SourceObj>> it = set.iterator();
			Object[] values = null;
			String[] targetFileds = new String[] { "target", "data_time"};
			//need create db index
			String[] procFileds = new String[] { "xmlid", "date_args", "task_state","valid_flag" };
			boolean needCreate = false;
			while (it.hasNext()) {
				Map.Entry<String, SourceObj> item = it.next();
				SourceObj obj = item.getValue();
				if(obj.getSourcefreq().equals(DataFreq.N.name())){
					continue;
				}
				if("PROC".equalsIgnoreCase(obj.getSourcetype())){
					values = new Object[] {  obj.getSource(), TimeUtils.getDependDateArgs(obj.getSourcefreq(),tl.getDateArgs(),StringUtils.equals(obj.getSourcetype(), ObjType.PROC.name())), RunStatus.PROC_RUN_SUCCESS, IsValid.VALID_FLAG };
					needCreate = search.checkExist("proc_schedule_log", procFileds, values) > 0;
				}else{
					values = new Object[] { obj.getSource(), TimeUtils.getDependDateArgs(obj.getSourcefreq(),tl.getDateArgs(),StringUtils.equals(obj.getSourcetype(), ObjType.PROC.name()))};
					needCreate = search.checkExist("proc_schedule_meta_log", targetFileds, values) > 0;
				}
				if(needCreate){
					storage.deleteTaskLog(tl.getSeqno());
					//重新创建起来
					this.createTaskRunInfo(MemCache.PROC_MAP.get(tl.getXmlid()), null, tl.getDateArgs());
					break;
				}
			}
		}
	}
	
	/**
	 * 是否在有效期内
	 * @param tc
	 * @return
	 */
	private boolean isTaskExpTimeInValid(TaskConfig tc){
		String nowStr = TimeUtils.date2String(new Date(), "yyyy-MM-dd");
		return nowStr.compareTo(tc.getEffTime()) >= 0 && nowStr.compareTo(tc.getExpTime()) < 0;
	}
	
	/**
	 * 任务有效期结束,销毁任务
	 * @param config
	 * @throws NumberFormatException
	 * @throws ParseException
	 * @throws Exception
	 */
	public void refurbishProcConfig2Dead(TaskConfig config) throws NumberFormatException, ParseException, Exception {
		String xmlid = config.getXmlid();
		MemCache.PROC_MAP.remove(xmlid);
		//时间触发刷新
		jobScheduleController.delscheduleJob(xmlid);
		
		//只处理当前有效的任务运行日志,已经无效的暂不处理
		for(TaskLog tl : search.getNeedrefurbishOldTaskLog(config.getXmlid())){
			storage.deleteTaskLog(tl.getSeqno());
		}
	}
	
	
	/**
	 * 清除任务相关依赖
	 * @param xmlid
	 */
	public void clearTaskCfgInTransdatamapDesign(String xmlid) {
		
		// 清除触发关系
		if (MemCache.TARGET_MAP.containsKey(xmlid)) {
			MemCache.TARGET_MAP.remove(xmlid);
		}
		
		// 清除依赖关系
		if (MemCache.SOURCE_MAP.containsKey(xmlid)) {
			MemCache.SOURCE_MAP.remove(xmlid);
		}

		// 清除触发包含关系
		Set<Entry<String, Map<String, TargetObj>>> set = MemCache.TARGET_MAP.entrySet();
		Iterator<Entry<String, Map<String, TargetObj>>> it = set.iterator();
		while (it.hasNext()) {
			Entry<String, Map<String, TargetObj>> item = it.next();
			String xmlidRelated = item.getKey();
			Set<Entry<String, TargetObj>> set_ = item.getValue().entrySet();
			Iterator<Entry<String, TargetObj>> it_ = set_.iterator();
			while (it_.hasNext()) {
				Entry<String, TargetObj> item_ = it_.next();
				if (item_.getKey().equalsIgnoreCase(xmlid)) {
					// 清除相关任务关系
					MemCache.TARGET_MAP.get(xmlidRelated).remove(xmlid);
				}
			}
		}

		// 清除依赖包含关系
		Set<Entry<String, Map<String, SourceObj>>> set_s = MemCache.SOURCE_MAP.entrySet();
		Iterator<Entry<String, Map<String, SourceObj>>> it_s = set_s.iterator();
		while (it_s.hasNext()) {
			Entry<String, Map<String, SourceObj>> item = it_s.next();
			String xmlidRelated = item.getKey();
			Set<Entry<String, SourceObj>> set_ = item.getValue().entrySet();
			Iterator<Entry<String, SourceObj>> it_ = set_.iterator();
			while (it_.hasNext()) {
				Entry<String, SourceObj> item_ = it_.next();
				if (item_.getKey().equalsIgnoreCase(xmlid)) {
					// 清除相关任务关系
					MemCache.SOURCE_MAP.get(xmlidRelated).remove(xmlid);
				}
			}
		}
		
		//清除SOURCE_EVENT
		Set<Entry<String, Map<String, Integer>>> set_se = MemCache.SOURCE_EVENT.entrySet();
		Iterator<Entry<String, Map<String, Integer>>> it_se = set_se.iterator();
		while (it_se.hasNext()) {
			Entry<String, Map<String, Integer>> item = it_se.next();
			String soureKey = item.getKey();
			Set<Entry<String, Integer>> set_ = item.getValue().entrySet();
			Iterator<Entry<String, Integer>> it_ = set_.iterator();
			while (it_.hasNext()) {
				String key = it_.next().getKey();
				if(key.contains(xmlid)){
					MemCache.SOURCE_EVENT.get(soureKey).remove(key);					
				}
			}
		}
		
	}
	
	
	/**
	 * 刷新任务关系
	 * @param xmlid
	 */
	private void refurbishTaskCfgInTransdatamapDesign(String xmlid){
		//清除触发关系中DATA数据
		if(MemCache.TARGET_MAP.containsKey(xmlid)){
			Set<Entry<String, TargetObj>> set = MemCache.TARGET_MAP.get(xmlid).entrySet();
			Iterator<Entry<String, TargetObj>> it = set.iterator();
			while (it.hasNext()) {
				Entry<String, TargetObj> item = it.next();
				if(!item.getValue().getTargettype().equals(ObjType.PROC.name())){
					MemCache.TARGET_MAP.get(xmlid).remove(item.getKey());
				}			
			}	
		}
		
		//清除依赖关系
		if(MemCache.SOURCE_MAP.containsKey(xmlid)){
			MemCache.SOURCE_MAP.remove(xmlid);
		}
		
		//清除触发包含关系
		Set<Entry<String, Map<String, TargetObj>>> set = MemCache.TARGET_MAP.entrySet();
		Iterator<Entry<String, Map<String, TargetObj>>> it = set.iterator();
		while (it.hasNext()) {
			Entry<String, Map<String, TargetObj>> item = it.next();
			String xmlidRelated = item.getKey();
			Set<Entry<String, TargetObj>> set_ = item.getValue().entrySet();
			Iterator<Entry<String, TargetObj>> it_ = set_.iterator();
			while (it_.hasNext()) {
				Entry<String, TargetObj> item_ = it_.next();
				if(item_.getKey().equalsIgnoreCase(xmlid)){
					//清除相关任务关系
					MemCache.TARGET_MAP.get(xmlidRelated).remove(xmlid);
				}
			}
		}
	}
	
	/**
	 * 刷新任务配置以及运行记录
	 * @param config
	 * @throws Exception 
	 * @throws ParseException 
	 * @throws NumberFormatException
	 */
	public void refurbishProcConfig2Old(TaskConfig config) throws NumberFormatException, ParseException, Exception {
		String xmlid = config.getXmlid();
		MemCache.PROC_MAP.remove(xmlid);
		MemCache.PROC_MAP.put(xmlid, config);
		tcs.refurbishTransdatamapDesign(xmlid);
		//时间触发刷新
		if(config.getTriggerType() == Type.DRIVE_TYPE.TIME_TRIGGER.ordinal()){
			jobScheduleController.rescheduleJob(config.getXmlid(), config.getCronExp());
		}else{
			jobScheduleController.delscheduleJob(config.getXmlid());
		}
		for(TaskLog tl : search.getNeedrefurbishOldTaskLog(config.getXmlid())){
			storage.deleteTaskLog(tl.getSeqno());
			//重新创建起来
			this.createTaskRunInfo(config, null, tl.getDateArgs());
		}
	}
	
	/**
	 * 重做任务后续优化
	 * @throws NumberFormatException
	 * @throws ParseException
	 * @throws Exception
	 */
	public void redoTaskFollowUpOptimize() throws NumberFormatException, ParseException, Exception {
		List<TaskLog> tls = search.queryRedoTaskRunLogList();
		if(tls == null || tls.isEmpty()){
			return;			
		}else{
			LOG.info("start to redo task:{}",tls.size());
		}
		//重做当前的任务
		for(TaskLog tl : tls){
			if(tl.getTriggerFlag()==1){
				LOG.info("start redoCur task:[{}]",tl.getTaskId());
				//MemCache.EVENT_SUC_FIFO.remove(tl.getXmlid() + "@" + tl.getDateArgs());
				storage.setTaskInvalid(tl.getSeqno()); // 失效任务
				this.createTaskRunInfo(MemCache.PROC_MAP.get(tl.getXmlid()), null, tl.getDateArgs(),false);
			}
		}
		List<String> allFollowUpTasks = new ArrayList<String>();
		for(TaskLog tl : tls){
			if(tl.getTriggerFlag()==1){
				continue;
			}
			List<FollowUpTask> l = this.redoFlowUpTasks(tl.getXmlid(),tl.getDateArgs());
			LOG.info("start redoFlowUp task:[{}],this task have [{}] FollowUpTask by Cfg",tl.getTaskId(),l == null? "0" : l.size());
			//优化合并,同账期后续相同则合并
			if(l ==null || l.isEmpty()){
				continue;
			}
			for(FollowUpTask tut : l){
				String key = StringUtils.join(tut.getXmlid(), "#", tut.getDateArgs());
				if(!allFollowUpTasks.contains(key)){
					allFollowUpTasks.add(key);
				}
			}
		}
		if(allFollowUpTasks.isEmpty()){
			return;
		}		
		//循环账期下需要重做的任务
		for(String key : allFollowUpTasks){
			String[] keys = key.split("#");
			LOG.info("now,redo task[{},{}]",keys[0],keys[1]);
			//MemCache.EVENT_SUC_FIFO.remove(keys[0] + "@" + keys[1]);
			if(MemCache.PROC_MAP.containsKey(keys[0])){
				this.redoTask(search.queryfollowUpTask(keys[0], keys[1]));
			}else{//输出数据类型
				this.delete2Meta(keys[0],keys[1]);
			}
		}		
		LOG.info("redo task all followUp task {} optimize is finish...",allFollowUpTasks.size());		
	}
	
	/**
	 * 重做任务
	 * @param ll
	 * @throws NumberFormatException
	 * @throws ParseException
	 * @throws Exception
	 */
	public void redoTask(TaskLog ll) throws NumberFormatException, ParseException, Exception{
		//如果无任务记录,不创建
		if(ll == null){
			return;
		}		
		switch (ll.getTaskState()) {
		case RunStatus.REDO:
		case RunStatus.CREATE_TASK:
		case 0 - RunStatus.CREATE_TASK:
		case RunStatus.CHECK_DEPEND_SUCCESS:
		case 0 - RunStatus.CHECK_DEPEND_SUCCESS:
		case RunStatus.CHECK_RUNMODEL_SUCCESS:
		case 0 - RunStatus.CHECK_RUNMODEL_SUCCESS:
			storage.deleteTaskLog(ll.getSeqno());// 检测状态的结果无意义，直接删除
			break;
		case RunStatus.PROC_RUN_SUCCESS:
		case RunStatus.PROC_RUN_FAIL:// 有执行结果的记录保留日志，失效任务
			storage.setTaskInvalid(ll.getSeqno()); // 失效任务
			break;
		case RunStatus.SEND_TO_MQ:
		case RunStatus.PROC_RUNNING:// 正在运行的任务，直接停止任务
			LOG.warn("coming soon be kill task[{}] by redo",ll.getTaskId());
			storage.setTaskInvalid(ll.getSeqno()); // 失效任务
			this.killProcess(ll);
			break;
		default:
			return;
		}
		
		TaskLog newtl = this.createTaskRunInfo(MemCache.PROC_MAP.get(ll.getXmlid()),null,ll.getDateArgs());//重做后续直接创建任务，检测依赖。
		LOG.info("old task[{}] tobe dead,new task[{}] tobe create",ll.getTaskId(),newtl== null ? "dirty read" : newtl.getTaskId());
	}	
	
	/**
	 * 重做后续查找后续任务
	 * @param xmlid
	 * @param dateArgs
	 * @return
	 * @throws Exception 
	 */
	private List<FollowUpTask> redoFlowUpTasks(String xmlid, String dateArgs) throws Exception{
		List<FollowUpTask> l = new ArrayList<FollowUpTask>();
		FollowUpTask fut = new FollowUpTask();
		fut.setXmlid(xmlid);
		fut.setSourceXmlid(xmlid);
		fut.setDateArgs(dateArgs);
		fut.setSourceArgs(dateArgs);
		fut.setLevel(0);
		l.add(fut);
		
		this.redoFlowUpTasks(xmlid, xmlid, dateArgs, l, new ConcurrentHashMap<String,Integer>(),new ConcurrentHashMap<String,List<String>>(),0);
		return l;
	}
		
	private void redoFlowUpTasks(String xmlid,String sourceXmlid, String dateArgs,List<FollowUpTask> l,
			Map<String,Integer> found, Map<String,List<String>> recursive,int level) {
		String key = StringUtils.join(xmlid, "#", dateArgs);
		if(!recursive.containsKey(key)){
			recursive.put(key, new ArrayList<String>());
		}
		//避免闭环
		if (!recursive.get(key).contains(sourceXmlid)){
			recursive.get(key).add(sourceXmlid);			
			if(MemCache.TARGET_MAP.get(xmlid) != null){
				Set<Map.Entry<String,TargetObj>> set = MemCache.TARGET_MAP.get(xmlid).entrySet();
				Iterator<Map.Entry<String,TargetObj>> it = set.iterator();
				while (it.hasNext()) {
					TargetObj toj = it.next().getValue();					
					if(toj.getSourcefreq().equals(DataFreq.N.name()) || toj.getTargetfreq().equals(DataFreq.N.name())){
						continue;
					}
					try {
					if(toj.getSourcefreq().contains("ML") || toj.getSourcefreq().contains("DL")){
						if(!TimeUtils.isLast(toj.getSourcefreq().contains("ML")? "day":"hour",dateArgs,!toj.getSourcetype().equalsIgnoreCase(ObjType.PROC.name()))){
							continue;
						}
					}
					if(toj.getSourcefreq().contains("DH") || toj.getSourcefreq().contains("DD")){
						if(!TimeUtils.isDesignated(toj.getSourcefreq().contains("DD")? "day":"hour",toj.getSourcefreq().split("#")[1],dateArgs,!toj.getSourcetype().equalsIgnoreCase(ObjType.PROC.name()))){
							continue;
						}
					}
					List<String> targetDateArgs = null;
					if(toj.getTargettype().equalsIgnoreCase(ObjType.PROC.name())){
						if(toj.getSourcetype().equalsIgnoreCase(ObjType.PROC.name())){
							targetDateArgs = this.getCycleReverse(toj, TimeUtils.getProcTargetProcDateArgs(toj.getSourcefreq(),dateArgs));
						}else{
							targetDateArgs = this.getCycleReverse(toj, TimeUtils.getDataTargetProcDateArgs(toj.getSourcefreq(),dateArgs));
						}
					}else{
						targetDateArgs = this.getCycleReverse(toj, TimeUtils.getProcTargetDataDataTime(toj.getTargetfreq(),dateArgs));
					}
					
					for(String tDateArgs : targetDateArgs){
						if(TimeUtils.isBefore2Tomorrow(tDateArgs) && 
								!found.containsKey(StringUtils.join(toj.getTarget(), "#", tDateArgs))){
							found.put(StringUtils.join(toj.getTarget(), "#", tDateArgs), 1);
							FollowUpTask fut = new FollowUpTask();
							fut.setXmlid(toj.getTarget());
							fut.setSourceXmlid(xmlid);
							fut.setDateArgs(tDateArgs);
							fut.setSourceArgs(dateArgs);
							fut.setLevel(level);							
							l.add(fut);
						}
					}
					
					for(String tDateArgs : targetDateArgs){
						if(TimeUtils.isBefore2Tomorrow(tDateArgs)){
							this.redoFlowUpTasks(toj.getTarget(), toj.getSource(), tDateArgs, l, found, recursive, level++);
						}
					}
					
					} catch (Exception e) {
						e.printStackTrace();
						LOG.error(e.getMessage());
					}
				}		
				
			}			
		}
	}
	
	/**
	 * 重做后续逆向分析(分钟任务不考虑)
	 * @return
	 * @throws Exception 
	 */
	private List<String> getCycleReverse(TargetObj toj, String targetDateArgs) throws Exception{
		List<String> l = new ArrayList<String>();
		if((toj.getSourcefreq().matches("^(M|ML).*") && toj.getTargetfreq().startsWith("D-")) ||
				(toj.getSourcefreq().matches("^(M|ML|D|DL).*") && toj.getTargetfreq().startsWith("H-"))){
			return TimeUtils.getCycleReverse(targetDateArgs);
		}
		l.add(targetDateArgs);
		return l;
	}
	
	
	/**
	 * 查找后续(结果包含自己)-for web
	 * @param xmlid
	 * @param sourceXmlid
	 * @param dateArgs
	 * @param found
	 * @param recursive
	 * @return
	 * @throws Exception
	 */
	public Map<String,Map<String,Map<String,String>>> getFlowInfo2After(String xmlid,String sourceXmlid,
			Map<String,Map<String,Map<String,String>>> found, Map<String,List<String>> recursive,int level,int levelSet) throws Exception{
		if(found == null){
			found = new ConcurrentHashMap<String,Map<String,Map<String,String>>>();
			found.put("NODE", new ConcurrentHashMap<String,Map<String,String>>());
			found.put("LINK", new ConcurrentHashMap<String,Map<String,String>>());
			
//			Map<String,String> node = new HashMap<String,String>();
//			node.put("key", xmlid);
//			node.put("text", MemCache.PROC_MAP.get(xmlid).getProcName());
//			node.put("color", "lightblue");
//			found.get("NODE").put(xmlid, node);
		}
		if(recursive == null){
			recursive = new ConcurrentHashMap<String,List<String>>();			
		}
		if(!recursive.containsKey(xmlid)){
			recursive.put(xmlid, new ArrayList<String>());
		}
		//避免闭环
		if (!recursive.get(xmlid).contains(sourceXmlid)){
			recursive.get(xmlid).add(sourceXmlid);
			if(MemCache.TARGET_MAP.get(xmlid) != null){
				Set<Map.Entry<String,TargetObj>> set = MemCache.TARGET_MAP.get(xmlid).entrySet();
				Iterator<Map.Entry<String,TargetObj>> it = set.iterator();
				while (it.hasNext()) {
					Map.Entry<String, TargetObj> item = it.next();
					TargetObj toj = item.getValue();
					if(!found.get("NODE").containsKey(toj.getTarget())){
						Map<String,String> node = new HashMap<String,String>();
						node.put("key", toj.getTarget());
						
						if(MemCache.PROC_MAP.containsKey(toj.getTarget())){
							node.put("text", MemCache.PROC_MAP.get(toj.getTarget()).getProcName() + "|" + MemCache.PROC_MAP.get(toj.getTarget()).getRunFreq());
						}else if(MemCache.EVENT_ATTRIBUTE.containsKey(toj.getTarget())){
							node.put("text", MemCache.EVENT_ATTRIBUTE.get(toj.getTarget()).getName() + "|" + MemCache.EVENT_ATTRIBUTE.get(toj.getTarget()).getCycleType());
						}else{
							node.put("text", toj.getTarget());
						}					
						switch (toj.getTargettype().toUpperCase()) {
						case "PROC":
							node.put("color", "lightblue");
							break;
						case "DATA":
							node.put("color", "lightgreen");
							break;
						case "INTER":
							node.put("color", "orange");
							break;
						default:
							node.put("color", "green");
							break;
						}
						found.get("NODE").put(toj.getTarget(), node);
					}
					if(!found.get("LINK").containsKey(xmlid.concat("#").concat(toj.getTarget()))){
						Map<String,String> link = new HashMap<String,String>();
						link.put("from", xmlid);
						link.put("to", toj.getTarget());
						found.get("LINK").put(xmlid.concat("#").concat(toj.getTarget()), link);
					}
					if(levelSet == -1 ||  level <= levelSet){
						level++;
						this.getFlowInfo2After(toj.getTarget(),xmlid,found,recursive,level,levelSet);
					}					
				}
			}			
		}
		return found;
	}
	
	/**
	 * 查找前置(结果包含自己)-for web
	 * @param xmlid
	 * @param sourceXmlid
	 * @param dateArgs
	 * @param found
	 * @param recursive
	 * @return
	 * @throws Exception
	 */
	public Map<String,Map<String,Map<String,String>>> getFlowInfo2Pre(String xmlid,String sourceXmlid,
			Map<String,Map<String,Map<String,String>>> found, Map<String,List<String>> recursive,int level,int levelSet) throws Exception{
		if(found == null){
			found = new ConcurrentHashMap<String,Map<String,Map<String,String>>>();
			found.put("NODE", new ConcurrentHashMap<String,Map<String,String>>());
			found.put("LINK", new ConcurrentHashMap<String,Map<String,String>>());
			
			Map<String,String> node = new HashMap<String,String>();
			node.put("key", xmlid);
			node.put("text", MemCache.PROC_MAP.get(xmlid).getProcName());
			node.put("color", "lightblue");			
			found.get("NODE").put(xmlid, node);
		}
		if(recursive == null){
			recursive = new ConcurrentHashMap<String,List<String>>();			
		}
		if(!recursive.containsKey(xmlid)){
			recursive.put(xmlid, new ArrayList<String>());
		}
		//避免闭环
		if (!recursive.get(xmlid).contains(sourceXmlid)){
			recursive.get(xmlid).add(sourceXmlid);
			if(MemCache.SOURCE_MAP.get(xmlid) != null){
				Set<Map.Entry<String,SourceObj>> set = MemCache.SOURCE_MAP.get(xmlid).entrySet();
				Iterator<Map.Entry<String,SourceObj>> it = set.iterator();
				while (it.hasNext()) {
					Map.Entry<String, SourceObj> item = it.next();
					SourceObj toj = item.getValue();
					if(!found.get("NODE").containsKey(toj.getSource())){
						Map<String,String> node = new HashMap<String,String>();
						node.put("key", toj.getSource());
						
						if(MemCache.PROC_MAP.containsKey(toj.getSource())){
							node.put("text", MemCache.PROC_MAP.get(toj.getSource()).getProcName() + "|" + MemCache.PROC_MAP.get(toj.getSource()).getRunFreq());
						}else if(MemCache.EVENT_ATTRIBUTE.containsKey(toj.getSource())){
							node.put("text", MemCache.EVENT_ATTRIBUTE.get(toj.getSource()).getName() + "|" + MemCache.EVENT_ATTRIBUTE.get(toj.getSource()).getCycleType());
						}else{
							node.put("text", toj.getSource());
						}					
						switch (toj.getSourcetype().toUpperCase()) {
						case "PROC":
							node.put("color", "lightblue");
							break;
						case "DATA":
							node.put("color", "lightgreen");
							break;
						case "INTER":
							node.put("color", "orange");
							break;
						default:
							node.put("color", "green");
							break;
						}
						found.get("NODE").put(toj.getSource(), node);
					}
					if(!found.get("LINK").containsKey(xmlid.concat("#").concat(toj.getSource()))){
						Map<String,String> link = new HashMap<String,String>();
						link.put("from", toj.getSource());
						link.put("to", xmlid);
						found.get("LINK").put(xmlid.concat("#").concat(toj.getSource()), link);
					}
					if(levelSet == -1 ||  level <= levelSet){
						level++;
						this.getFlowInfo2Pre(toj.getSource(),xmlid,found,recursive,level,levelSet);
					}					
				}
			}			
		}
		return found;
	}
	
	/**
	 * 查找未触发的原因
	 * @param xmlid
	 * @param dateArgs
	 * @param found
	 * @param recursive
	 * @return
	 * @throws Exception
	 */
	public List<SourceLog> getReasonsOfNoTrigger(String xmlid, String dateArgs, List<SourceLog> found, Map<String, String> recursive, int level,int setLevel) throws Exception{		
		if(found == null){
			found = new ArrayList<SourceLog>();
		}
		if(recursive == null){
			recursive = new ConcurrentHashMap<String,String>();			
		}
		//避免闭环
		if (!recursive.containsKey(xmlid)){
			recursive.put(xmlid, "");
			if(MemCache.SOURCE_MAP.get(xmlid) != null){
				String[] targetFileds = new String[] { "target", "data_time"};
				String[] procFileds = new String[] { "xmlid", "date_args", "task_state","valid_flag" };	
				Set<Map.Entry<String,SourceObj>> set = MemCache.SOURCE_MAP.get(xmlid).entrySet();
				Iterator<Map.Entry<String,SourceObj>> it = set.iterator();
				while (it.hasNext()) {
					Map.Entry<String, SourceObj> item = it.next();
					SourceObj obj = item.getValue();
					if(obj.getSourcefreq().equals(DataFreq.N.name())){
						continue;
					}
					SourceLog srcLog = new SourceLog();
					if(StringUtils.equals(obj.getSourcetype(), ObjType.PROC.name())){
						srcLog.setSource(MemCache.PROC_MAP.get(obj.getSource()).getProcName());
						srcLog.setCycleType(MemCache.PROC_MAP.get(obj.getSource()).getRunFreq());
					}else if(MemCache.EVENT_ATTRIBUTE.containsKey(obj.getSource())){
						srcLog.setSource(MemCache.EVENT_ATTRIBUTE.get(obj.getSource()).getName());
						srcLog.setCycleType(MemCache.EVENT_ATTRIBUTE.get(obj.getSource()).getCycleType());
					}
					srcLog.setSourceType(obj.getSourcetype());
					srcLog.setDataTime(TimeUtils.getDependDateArgs(obj.getSourcefreq(),dateArgs,StringUtils.equals(obj.getSourcetype(), ObjType.PROC.name())));
					srcLog.setCheckFlag(StringUtils.equals(obj.getSourcetype(), ObjType.PROC.name()) ?
							search.checkExist("proc_schedule_log", procFileds, new Object[]{srcLog.getSource(), srcLog.getDataTime(), RunStatus.PROC_RUN_SUCCESS, IsValid.VALID_FLAG}) : search.checkExist("proc_schedule_meta_log", targetFileds, new Object[]{srcLog.getSource(), srcLog.getDataTime()}));//TODO 这里需要改造的		
					if(srcLog.getCheckFlag()==0 && (setLevel == -1 || level <= setLevel)){
						srcLog.setLevel(level);
						found.add(srcLog);
						this.getReasonsOfNoTrigger(obj.getSource(), srcLog.getDataTime(), found, recursive,level++,setLevel);
					}
				}				
			}			
		}
		return found;
	}
	
	/**
	 * 杀掉后台任务进程(异步)
	 * @param run
	 */
	public boolean killProcess(TaskLog task) {
		if(MemCache.AGENT_IPS_MAP.get(task.getAgentCode()).getAgentStatus() != 1){
			LOG.error("task[{}] run kill process failed!,because agent is dead",task.getTaskId());
			return false;
		}
		boolean isKill = false;
		DpMessage message = new DpMessage();
		Map<String, String> map = new HashMap<String, String>();
		map.put("SEQNO", task.getSeqno());
		map.put("AGENT_CODE", task.getAgentCode());
		message.setMsgType("KILL_PROC");
		message.setMsgId(task.getSeqno());
		message.setClassUrl("default-url");
		message.setClassMethod("default-method");
		message.setSourceQueue("taskServer");
		message.addBody(map);
		Object delRes = this.getDpSender().sendAndRecieve(task.getAgentCode() + "_REQUEST_QUEUE", message, 1000 * 10);
		isKill = delRes != null && delRes.equals("true");
		if (isKill) {
			LOG.info("task[{}] run kill process success!",task.getTaskId());
		} else {
			LOG.error("task[{}] run kill process failed!",task.getTaskId());
		}
		return isKill;
	}
	
	
	/**
	 * 生成待触发任务运行信息
	 * @param config
	 * @param baseDate 全新创建日期
	 * @return
	 * @throws Exception 
	 * @throws ParseException 
	 * @throws NumberFormatException 
	 */
	public TaskLog createStandbyTaskRunInfo(TaskConfig config, Date baseDate) throws NumberFormatException, ParseException, Exception {
		Date now = baseDate == null ? new Date() : baseDate;
		TaskLog runInfo = new TaskLog();
		String seqno = UUIDUtils.getUUID();		
		runInfo.setSeqno(seqno);
		runInfo.setXmlid(config.getXmlid());
		runInfo.setAgentCode(config.getAgentCode());
		runInfo.setRunFreq(config.getRunFreq());
		runInfo.setProcName(config.getProcName());		
		runInfo.setPlatform(config.getPlatform());
		runInfo.setTaskState(RunStatus.PLAN_TASK);
		runInfo.setStatusTime(TimeUtils.date2String(now, "yyyy-MM-dd HH:mm:ss"));
		runInfo.setTimeWin(config.getTimeWin());
		runInfo.setTeamCode(config.getTeamCode());
		runInfo.setPriLevel(config.getPriLevel());
		// 不入队
		runInfo.setQueueFlag(1);
		runInfo.setTriggerFlag(0);
		runInfo.setValidFlag(0);
		// 流程号
		runInfo.setFlowcode(config.getFlowcode());
		runInfo.setPath(config.getPath());
		runInfo.setProctype(config.getProcType());
		runInfo.setPreCmd(config.getPreCmd());//初始化前置命令拼接
		runInfo.setProcDate(TimeUtils.date2String(now, "yyyyMMddHHmm"));
		runInfo.setDateArgs(TimeUtils.getDateArgs(config.getRunFreq(), now,0));
		
//		if(search.isTaskExist2Standby(config.getXmlid(), runInfo.getDateArgs()) ==1){
//			storage.validTask(config.getXmlid(), runInfo.getDateArgs());
//		} 
//		storage.insertTaskLog(runInfo);
		if(search.isTaskExist2Standby(config.getXmlid(), runInfo.getDateArgs()) ==1){
			LOG.info("task[{}] has  exist  do nothing",runInfo.getTaskId());
		} else {
			storage.insertTaskLog(runInfo);
		}
		return runInfo;
	}
	
	/**
	 * 创建是否触发后续的任务类型
	 * @param config
	 * @param baseDate
	 * @param redoDateArgs
	 * @param isTrigger
	 * @return
	 * @throws NumberFormatException
	 * @throws ParseException
	 * @throws Exception
	 */
	public TaskLog createTaskRunInfo(TaskConfig config, Date baseDate,String redoDateArgs,boolean isTrigger) throws NumberFormatException, ParseException, Exception {
		return this.createTaskRunInfoPrimary(config, baseDate, redoDateArgs,RunStatus.CREATE_TASK,isTrigger);
	}
	
	/**
	 * 基础创建任务
	 * @param config
	 * @param baseDate
	 * @param specifyDateArgs
	 * @return
	 * @throws NumberFormatException
	 * @throws ParseException
	 * @throws Exception
	 */
	public TaskLog createTaskRunInfo(TaskConfig config, Date baseDate,String specifyDateArgs) throws NumberFormatException, ParseException, Exception {		
		return this.createTaskRunInfoPrimary(config, baseDate, specifyDateArgs,RunStatus.CREATE_TASK,true);
	}
	
	
	/**
	 * 创建任务,状态创建成功
	 * @param config
	 * @param baseDate 时间类型任务用
	 * @param specifyDateArgs 指定任务DateArgs
	 * @return
	 * @throws Exception 
	 * @throws ParseException 
	 * @throws NumberFormatException 
	 */
	public TaskLog createTaskRunInfoPrimary(TaskConfig config, Date baseDate,String specifyDateArgs,int TaskState,boolean isTrigger) throws NumberFormatException, ParseException, Exception {
		Date now = baseDate == null ? new Date() : baseDate;
		TaskLog runInfo = new TaskLog();
		String seqno = UUIDUtils.getUUID();		
		runInfo.setSeqno(seqno);
		runInfo.setXmlid(config.getXmlid());
		runInfo.setAgentCode(config.getAgentCode());
		runInfo.setRunFreq(config.getRunFreq());
		runInfo.setProcName(config.getProcName());		
		runInfo.setPlatform(config.getPlatform());
		runInfo.setTaskState(TaskState);	
		runInfo.setStatusTime(TimeUtils.date2String(now, "yyyy-MM-dd HH:mm:ss"));
		runInfo.setStartTime(TimeUtils.date2String(now, "yyyy-MM-dd HH:mm:ss"));
		runInfo.setTimeWin(config.getTimeWin());
		runInfo.setTeamCode(config.getTeamCode());
		runInfo.setPriLevel(config.getPriLevel());
		// 入队
		runInfo.setQueueFlag(0);
		runInfo.setTriggerFlag(isTrigger ? 0 :1);
		runInfo.setValidFlag(0);
		// 流程号
		runInfo.setFlowcode(config.getFlowcode());
		runInfo.setPath(config.getPath());
		runInfo.setProctype(config.getProcType());
		runInfo.setPreCmd(config.getPreCmd());//初始化前置命令拼接
		if(StringUtils.isEmpty(specifyDateArgs)){
			runInfo.setProcDate(TimeUtils.date2String(now, "yyyyMMddHHmm"));
			runInfo.setDateArgs(TimeUtils.getDateArgs(config.getRunFreq(), now, Integer.parseInt(config.getDateArgs())));
		}else{
			runInfo.setDateArgs(specifyDateArgs);
		}
		if(search.isTaskExist(config.getXmlid(), runInfo.getDateArgs()) ==1){
			LOG.info("can't create task:[{}],cause is exist", runInfo.getTaskId());
			return null;
		}
		storage.insertTaskLog(runInfo);
		storage.insertSourceLog(MemCache.SOURCE_MAP.get(config.getXmlid()),seqno,runInfo.getXmlid(),runInfo.getDateArgs());
		
		String rmKey = runInfo.getXmlid() + "@" + runInfo.getDateArgs();
		if(MemCache.RUNMODE_EVENT.containsKey(rmKey)){
			MemCache.RUNMODE_EVENT.remove(rmKey);
		}
		return runInfo;
	}
}
