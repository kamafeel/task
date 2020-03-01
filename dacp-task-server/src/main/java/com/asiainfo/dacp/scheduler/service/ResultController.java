package com.asiainfo.dacp.scheduler.service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.asiainfo.dacp.dp.common.RunStatus;
import com.asiainfo.dacp.dp.server.scheduler.bean.BaseResult;
import com.asiainfo.dacp.dp.server.scheduler.bean.SourceLog;
import com.asiainfo.dacp.dp.server.scheduler.bean.SourceObj;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskConfig;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskLog;
import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbSearch;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbStorage;
import com.asiainfo.dacp.dp.server.scheduler.type.DataFreq;
import com.asiainfo.dacp.dp.server.scheduler.type.ObjType;
import com.asiainfo.dacp.dp.tools.TimeUtils;
import com.google.gson.Gson;


/**
 * 调度对外服务jettey
 * TODO 对外部系统的服务应该改为 restful 模式
 * @author zhangqi
 *
 */
@Controller
@RequestMapping("/scheduleManager")
public class ResultController {
	
	@Autowired
	private TaskEventRdbSearch search;
	@Autowired
	private TaskEventRdbStorage storage;
	@Autowired
	private TaskService ts;
	
	private static Logger LOG = LoggerFactory.getLogger(ResultController.class);
	
	/**
	 * DEBUG_
	 * @return
	 */
	@RequestMapping("/synCache/debug/map")
	public @ResponseBody BaseResult debugMap(String type,String xmlid,String dateArgs) {
		BaseResult result = new BaseResult();
		result.setReCode(200);
		switch (type) {
			case "t":
				result.setReMsg(MemCache.TARGET_MAP.containsKey(xmlid) ? new Gson().toJson(MemCache.TARGET_MAP.get(xmlid)) : "");
				break;
			case "s":
				result.setReMsg(MemCache.SOURCE_MAP.containsKey(xmlid) ? new Gson().toJson(MemCache.SOURCE_MAP.get(xmlid)) : "");
				break;
			case "m":
				result.setReMsg(MemCache.RUNMODE_EVENT.containsKey(xmlid + "@" + dateArgs) ? new Gson().toJson(MemCache.RUNMODE_EVENT.get(xmlid + "@" + dateArgs)) : "");
				break;
			case "i":
				result.setReMsg(MemCache.PROC_MAP.containsKey(xmlid) ? new Gson().toJson(MemCache.PROC_MAP.get(xmlid)) : "");
				break;
		}
		return result;
	}
	
	/**
	 * KILL 任务
	 * @return
	 */
	@RequestMapping("/synCache/kill")
	public @ResponseBody BaseResult killRunningTask(String seqno) {
		BaseResult result = new BaseResult();
		result.setReCode(200);
		if (StringUtils.isEmpty(seqno)) {
			result.setReCode(400);
			result.setReMsg("seqno不能为空");
			return result;
		}
		TaskLog tl = search.queryTaskRunLog(seqno);
		LOG.info("task [{}] will be kill from web",tl.getTaskId());
		try {
	        if (ts.killProcess(tl)) {
	        	result.setReMsg("kill process success");
	        } else {
	        	result.setReCode(400);
	        	result.setReMsg("kill process fail");
	        }	        
		} catch (Exception e) {
			LOG.error("task [{}] kill is fail",tl.getTaskId(),e);
			result.setReCode(400);
			result.setReMsg("KILL Fail");
		}
		return result;
	}
	
	/**
	 * 分析任务流程
	 * @return
	 */
	@RequestMapping("/synCache/getFlowInfo")
	public @ResponseBody BaseResult getFlowInfo(String xmlid,int level,String analysisDirection) {
		
		BaseResult result = new BaseResult();
		result.setReCode(200);
		if (StringUtils.isEmpty(xmlid)) {
			result.setReCode(400);
			result.setReMsg("xmlid不能为空");
			return result;
		}
		if (!MemCache.PROC_MAP.containsKey(xmlid)) {
			result.setReCode(400);
			result.setReMsg("xmlid无法找到程序");
			return result;
		}
		try {
			Map<String,List<Map<String,String>>> returnMap = new ConcurrentHashMap<String,List<Map<String,String>>>();
			List<Map<String,String>> node = new ArrayList<Map<String,String>>();
			List<Map<String,String>> link = new ArrayList<Map<String,String>>();
			
			if(analysisDirection.equalsIgnoreCase("after")){							
				Map<String, Map<String, Map<String, String>>> m = ts.getFlowInfo2After(xmlid, xmlid, null, null, 0,level);
				Iterator<Map.Entry<String, Map<String, String>>> entries_node = m.get("NODE").entrySet().iterator();
				while (entries_node.hasNext()) {
					node.add(entries_node.next().getValue());
				}
				Iterator<Map.Entry<String, Map<String, String>>> entries_link = m.get("LINK").entrySet().iterator();
				while (entries_link.hasNext()) {
					link.add(entries_link.next().getValue());
				}
			}else if(analysisDirection.equalsIgnoreCase("pre")){
				Map<String, Map<String, Map<String, String>>> m = ts.getFlowInfo2Pre(xmlid, xmlid, null, null, 0,level);
				Iterator<Map.Entry<String, Map<String, String>>> entries_node = m.get("NODE").entrySet().iterator();
				while (entries_node.hasNext()) {
					node.add(entries_node.next().getValue());
				}
				Iterator<Map.Entry<String, Map<String, String>>> entries_link = m.get("LINK").entrySet().iterator();
				while (entries_link.hasNext()) {
					link.add(entries_link.next().getValue());
				}
			}
			returnMap.put("NODE", node);
			returnMap.put("LINK", link);
			result.setReInfo(returnMap);
		} catch (Exception e) {
			LOG.error("task [{}] getFlowInfo is fail",xmlid,e);
			result.setReCode(400);
			result.setReMsg("获取失败");
		}
		return result;
	}
	
	
	/**
	 * 分析未触发的原因
	 * @return
	 */
	@RequestMapping("/synCache/analysisNotriggerReason")
	public @ResponseBody BaseResult analysisNotriggerReason(String xmlid,String dateArgs,int level) {
		BaseResult result = new BaseResult();
		result.setReCode(200);
		if (StringUtils.isEmpty(xmlid)) {
			result.setReCode(400);
			result.setReMsg("xmlid不能为空");
			return result;
		}
		if (StringUtils.isEmpty(dateArgs)) {
			result.setReCode(400);
			result.setReMsg("dateArgs不能为空");
			return result;
		}
		
		try {
			result.setReInfo(ts.getReasonsOfNoTrigger(xmlid, dateArgs, null, null,0,level));			
		} catch (Exception e) {
			LOG.error("task [{}] analysisNotriggerReason is fail",xmlid,e);
			result.setReCode(400);
			result.setReMsg("获取失败");
		}
		return result;
	}
	
	
	/**
	 * 立即执行,不考虑agent并发
	 * @return
	 */
	@RequestMapping("/synCache/executionWithoutDelay")
	public @ResponseBody BaseResult executionWithoutDelay(String seqno) {
		BaseResult result = new BaseResult();
		result.setReCode(200);
		if (StringUtils.isEmpty(seqno)) {
			result.setReCode(400);
			result.setReMsg("seqno不能为空");
			return result;
		}
		TaskLog tl = search.queryTaskRunLog(seqno);
		LOG.info("task [{}] will be executionWithoutDelay from web",tl.getTaskId());
		try {
			if (!ts.sendProcToAgent(tl)) {
				MemCache.TASK_WAIT_REASON.put(tl.getSeqno(), RunStatus.AGENT.FAIL);
			}else{
				result.setReMsg("Send to Agent suc");
				MemCache.TASK_WAIT_REASON.remove(tl.getSeqno());
			}
		} catch (Exception e) {
			LOG.error("task [{}] executionWithoutDelay is fail",seqno,e);
			result.setReCode(400);
			result.setReMsg("ExecutionWithoutDelay is fail");
		}
		return result;
	}
	
	
	/**
	 * 获取排队等待原因
	 * @param seqno
	 * @return
	 */
	@RequestMapping("/synCache/getWaitCode")
	public @ResponseBody BaseResult getWaitCode(String seqno) {
		BaseResult result = new BaseResult();
		if (StringUtils.isEmpty(seqno)) {
			result.setReCode(400);
			result.setReMsg("查询任务等待原因seqno不能为空");
			return result;
		}		
		result.setReCode(200);
		result.setReMsg(MemCache.TASK_WAIT_REASON.get(seqno) == null ? "unknow" : MemCache.TASK_WAIT_REASON.get(seqno).toString());
		return result;
	}
	
	
	/**
	 * 获取未触发任务的依赖条件
	 * @param seqno
	 * @return
	 */
	@RequestMapping("/synCache/getCondiNotrigger")
	public @ResponseBody BaseResult getCondiNotrigger(String xmlid,String dateArgs) {
		BaseResult result = new BaseResult();
		if (StringUtils.isEmpty(xmlid) || StringUtils.isEmpty(dateArgs) ) {
			result.setReCode(400);
			result.setReMsg("xmlid和DateArgs不能为空");
			return result;
		}
		
		List<SourceLog> srcLogList = new ArrayList<SourceLog>();
		try {
			if(MemCache.SOURCE_MAP.containsKey(xmlid)){
				Set<Map.Entry<String, SourceObj>> set = MemCache.SOURCE_MAP.get(xmlid).entrySet();
				Iterator<Map.Entry<String, SourceObj>> it = set.iterator();
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
					srcLog.setCheckFlag(0);			
					srcLogList.add(srcLog);		
				}
			}			
		} catch (Exception e) {
			LOG.error("task [{}] get condi notrigger is fail",xmlid,e);
			result.setReCode(400);
			result.setReMsg("获取失败");
		}

		result.setReCode(200);
		result.setReMsg("suc");
		result.setReInfo(srcLogList);
		return result;
	}
	
	
	
	/**
	 * 手工创建任务
	 * @param xmlid
	 * @param dateArgs
	 * @return
	 */
	@RequestMapping(value={"/synCache/addTaskLogs","/synCache/addNew"})
	public @ResponseBody BaseResult addTaskLogs(String xmlid, String dateArgs) {
		BaseResult result = new BaseResult();
		if (StringUtils.isEmpty(xmlid)) {
			result.setReCode(400);
			result.setReMsg("xmlid不能为空");
			return result;
		}
		if (StringUtils.isEmpty(dateArgs)) {
			result.setReCode(400);
			result.setReMsg("dateArgs不能为空");
			return result;
		}
		LOG.info("task [{},{}] will be addNew from web",xmlid,dateArgs);
		try {
			String[] dates = dateArgs.split(",");
			for(String s : dates){
				TaskConfig tc = MemCache.PROC_MAP.get(xmlid);
				ts.createStandbyTaskRunInfo(tc, TimeUtils.formatStr2Date(tc.getRunFreq(),s));
			}
			result.setReCode(200);
			result.setReMsg("执行成功");
		} catch (Exception e) {
			LOG.error("task [{}] will be addNew from web is fail",xmlid,e);
			result.setReCode(400);
			result.setReMsg("执行失败");
		}		
		return result;
	}
		
	/**
	 * 重做当前
	 * @param seqno
	 * @param invalidSeqno
	 * @return
	 */
	@RequestMapping("/synCache/redoCur")
	public @ResponseBody BaseResult redoCur(String invalidSeqno, int returnCode) {
		BaseResult result = new BaseResult();
		if (StringUtils.isEmpty(invalidSeqno)) {
			result.setReCode(400);
			result.setReMsg("重做当前的任务Seqno不能为空");
			return result;
		}
		TaskLog tl = search.queryTaskRunLog(invalidSeqno);
		if(tl == null){
			result.setReCode(400);
			result.setReMsg(invalidSeqno + "不是有效的任务记录");
			return result;
		}
		
		if(!MemCache.PROC_MAP.containsKey(tl.getXmlid())){
			result.setReCode(400);
			result.setReMsg(invalidSeqno + "的任务配置无效");
			return result;
		}
		if(MemCache.RUN_TASK.get(tl.getXmlid())!=null&&MemCache.RUN_TASK.get(tl.getXmlid()).contains(tl.getDateArgs())) {
			result.setReCode(400);
			result.setReMsg(tl.getXmlid()+":"+tl.getDateArgs()+"正在运行中，请先停止任务");
			return result;
		}
		if(tl.getTaskState().equals(RunStatus.REDO)||tl.getTaskState().equals(RunStatus.CREATE_TASK)||tl.getTaskState().equals(RunStatus.CHECK_DEPEND_SUCCESS)||tl.getTaskState().equals(RunStatus.CHECK_RUNMODEL_SUCCESS)) {
			result.setReCode(400);
			result.setReMsg(tl.getXmlid()+":"+tl.getDateArgs()+"任务不为成功或者失败状态，无法执行重做");
			return result;
		}
		
		LOG.info("task [{}] will be redoCur from web",tl.getTaskId());
		try {
			storage.updateTaskStateRedoCurOrAfter(invalidSeqno,false);
			if(returnCode > 0){
				if(tl.getReturnCode() != null){
					MemCache.REDO_FROM_ERRORCODE.put(tl.getXmlid()+"@"+tl.getDateArgs(),tl.getReturnCode());
				}else{
					result.setReCode(400);
					result.setReMsg(invalidSeqno + "的错误步骤号为空,无法执行从错误步骤开始");
				}
			}
			result.setReCode(200);
			result.setReMsg("执行成功");
		} catch (Exception e) {
			LOG.error("task [{}] will be redoCur from web is fail",tl.getTaskId(),e);
			result.setReCode(400);
			result.setReMsg("执行失败");
		}		
		return result;
	}
	
	/**
	 * 重做后续
	 * @param invalidSeqno
	 * @return
	 */
	@RequestMapping("/synCache/redoAfter")
	public @ResponseBody BaseResult redoAfter(String invalidSeqno, int returnCode) {
		BaseResult result = new BaseResult();
		if (StringUtils.isEmpty(invalidSeqno)) {
			result.setReCode(400);
			result.setReMsg("重做后续，invalidSeqno不能为空");
			return result;
		}
		TaskLog tl =search.queryTaskRunLog(invalidSeqno);
		if(tl == null){
			result.setReCode(400);
			result.setReMsg(invalidSeqno + "不是有效的任务记录");
			return result;
		}
		
		if(!MemCache.PROC_MAP.containsKey(tl.getXmlid())){
			result.setReCode(400);
			result.setReMsg(invalidSeqno + "的任务配置无效");
			return result;
		}
		if(MemCache.RUN_TASK.get(tl.getXmlid())!=null&&MemCache.RUN_TASK.get(tl.getXmlid()).contains(tl.getDateArgs())) {
			result.setReCode(400);
			result.setReMsg(tl.getXmlid()+":"+tl.getDateArgs()+"正在运行中，请先停止任务");
			return result;
		}
		if(tl.getTaskState().equals(RunStatus.REDO)||tl.getTaskState().equals(RunStatus.CREATE_TASK)||tl.getTaskState().equals(RunStatus.CHECK_DEPEND_SUCCESS)||tl.getTaskState().equals(RunStatus.CHECK_RUNMODEL_SUCCESS)) {
			result.setReCode(400);
			result.setReMsg(tl.getXmlid()+":"+tl.getDateArgs()+"任务不为成功或者失败状态，无法执行重做");
			return result;
		}
		LOG.info("task [{}] will be redoAfter from web",tl.getTaskId());
		try {
			storage.updateTaskStateRedoCurOrAfter(invalidSeqno,true);
			if(returnCode > 0){
				if(tl.getReturnCode() != null){
					MemCache.REDO_FROM_ERRORCODE.put(tl.getXmlid()+"@"+tl.getDateArgs(),tl.getReturnCode());
				}else{
					result.setReCode(400);
					result.setReMsg(invalidSeqno + "的错误步骤号为空,无法执行从错误步骤开始,将默认从初始步骤开始");
				}
			}
			result.setReCode(200);
			result.setReMsg("执行成功");
		} catch (Exception e) {
			LOG.error("task [{}] will be redoAfter from web is fail",tl.getTaskId(),e);
			result.setReCode(400);
			result.setReMsg("执行失败");
		}		
		return result;
	}
	
	/**
	 * 强制执行
	 * @param seqno
	 * @return
	 */
	@RequestMapping("/synCache/forceExec")
	public @ResponseBody BaseResult forceExec(String seqno) {
		BaseResult result = new BaseResult();
		if (StringUtils.isEmpty(seqno)) {
			result.setReCode(400);
			result.setReMsg("强制执行的任务seqno不能为空");
			return result;
		}
		TaskLog tl =search.queryTaskRunLog(seqno);
		if(tl == null){
			result.setReCode(400);
			result.setReMsg(seqno + "不是有效的任务记录");
			return result;
		}
		
		if(!MemCache.PROC_MAP.containsKey(tl.getXmlid())){
			result.setReCode(400);
			result.setReMsg(seqno + "的任务配置无效");
			return result;
		}
		if(tl.getTaskState()== RunStatus.PLAN_TASK){
			try {
				tl = ts.createTaskRunInfo(MemCache.PROC_MAP.get(tl.getXmlid()), null, tl.getDateArgs());
				if(tl == null){
					result.setReCode(400);
					result.setReMsg("seqno:" + "已存在有效记录");
					return result;
				}
				seqno = tl.getSeqno();
			} catch (Exception e) {
				LOG.error("task [{}] will be forceExec from web is fail",tl.getTaskId(),e);
				result.setReCode(400);
				result.setReMsg("执行失败");
				return result;
			} 
		}
		LOG.info("task [{}] will be forceExec from web",tl.getTaskId());
		storage.updateTaskState(seqno, RunStatus.CHECK_DEPEND_SUCCESS);	
		result.setReCode(200);
		result.setReMsg("执行成功");
		return result;
	}
	
	/**
	 * 强制通过
	 * @param seqno
	 * @return
	 */
	@RequestMapping("/synCache/forcePass")
	public @ResponseBody BaseResult forcePass(String seqno) {
		BaseResult result = new BaseResult();
		if (StringUtils.isEmpty(seqno)) {
			result.setReCode(400);
			result.setReMsg("seqno不能为空");
			return result;
		}
		TaskLog tl =search.queryTaskRunLog(seqno);
		if(tl == null){
			result.setReCode(400);
			result.setReMsg(seqno + "不是有效的任务记录");
			return result;
		}
		
		if(!MemCache.PROC_MAP.containsKey(tl.getXmlid())){
			result.setReCode(400);
			result.setReMsg(seqno + "的任务配置无效");
			return result;
		}
		LOG.info("task [{}] will be forcePass from web",tl.getTaskId());
		storage.updateTaskState2forcePass(seqno, RunStatus.PROC_RUN_SUCCESS);
		TaskCacheService.removeRunTaskTemp(tl.getXmlid(),tl.getDateArgs());
		result.setReCode(200);
		result.setReMsg("执行成功");
		return result;
	}
	
	/**
	 * 设置优先级
	 * @param seqno
	 * @param priLevel
	 * @return
	 */
	@RequestMapping("/synCache/setPriLevel")
	public @ResponseBody BaseResult setPriLevel(String seqno, String priLevel) {
		BaseResult result = new BaseResult();
		if (StringUtils.isEmpty(seqno)) {
			result.setReCode(400);
			result.setReMsg("seqno不能为空");
			return result;
		}
		if (StringUtils.isEmpty(priLevel)) {
			result.setReCode(400);
			result.setReMsg("priLevel不能为空");
			return result;
		}
		if (!TimeUtils.isNumeric(priLevel)) {
			result.setReCode(400);
			result.setReMsg("priLevel只能为数字");
			return result;
		}
		TaskLog tl =search.queryTaskRunLog(seqno);
		if(tl == null){
			result.setReCode(400);
			result.setReMsg(seqno + "不是有效的任务记录");
			return result;
		}
		
		if(!MemCache.PROC_MAP.containsKey(tl.getXmlid())){
			result.setReCode(400);
			result.setReMsg(seqno + "的任务配置无效");
			return result;
		}
		if(tl.getTaskState() == RunStatus.CREATE_TASK || tl.getTaskState() == RunStatus.CHECK_DEPEND_SUCCESS
				|| tl.getTaskState() == RunStatus.CHECK_RUNMODEL_SUCCESS){			
			LOG.info("task [{},{}] will be setPriLevel from web",tl.getTaskId(),priLevel);
			storage.updateTaskPriLevel(seqno, Integer.parseInt(priLevel));
			result.setReCode(200);
			result.setReMsg("执行成功");			
		}else{
			result.setReCode(400);
			result.setReMsg(seqno + "的任务状态不允许调整优先级");
		}
		return result;
	}
	
	/**
	 * 暂停任务
	 * @param seqno
	 * @return
	 */
	@RequestMapping("/synCache/pauseTask")
	public @ResponseBody BaseResult pauseTask(String seqno) {
		BaseResult result = new BaseResult();
		LOG.info("task [{}] request pauseTask",seqno);
		if (StringUtils.isEmpty(seqno)) {
			result.setReCode(400);
			result.setReMsg("seqno不能为空");
			return result;
		}
		TaskLog tl =search.queryTaskRunLog(seqno);
		if(tl == null){
			result.setReCode(400);
			result.setReMsg(seqno + "不是有效的任务记录");
			return result;
		}
		
		if(!MemCache.PROC_MAP.containsKey(tl.getXmlid())){
			result.setReCode(400);
			result.setReMsg(seqno + "的任务配置无效");
			return result;
		}
		MemCache.PROC_PAUSE_MAP.put(tl.getSeqno(), 1);
		storage.updateTaskState(seqno, 0 - tl.getTaskState());	
		TaskCacheService.removeRunTaskTemp(tl.getXmlid(),tl.getDateArgs());
		result.setReCode(200);
		result.setReMsg("执行成功," + (MemCache.PROC_MAP.get(tl.getXmlid()).getTriggerType()==0 ? "时间触发的下一批次并不会暂停哦,只是暂停当前批次任务" : ""));
		return result;
	}
	
	/**
	 * 恢复任务
	 * @param seqno
	 * @return
	 */
	@RequestMapping("/synCache/recoverTask")
	public @ResponseBody BaseResult recoverTask(String seqno) {
		LOG.info("task [{}] request recoverTask",seqno);
		BaseResult result = new BaseResult();
		if (StringUtils.isEmpty(seqno)) {
			result.setReCode(400);
			result.setReMsg("seqno不能为空");
			return result;
		}
		TaskLog tl =search.queryTaskRunLog(seqno);
		if(tl == null){
			result.setReCode(400);
			result.setReMsg(seqno + "不是有效的任务记录");
			return result;
		}
		
		if(!MemCache.PROC_MAP.containsKey(tl.getXmlid())){
			result.setReCode(400);
			result.setReMsg(seqno + "的任务配置无效");
			return result;
		}
		MemCache.PROC_PAUSE_MAP.remove(tl.getSeqno());
		// 恢复任务后再去检测执行模式
		storage.updateTaskState(seqno, 2);
		result.setReCode(200);
		result.setReMsg("执行成功");
		return result;
	}
	
	/**
	 * 手工任务
	 * @param seqno
	 * @return
	 */
	@RequestMapping("/synCache/manualTask")
	public @ResponseBody BaseResult manualTask(String seqno) {
		BaseResult result = new BaseResult();
		if (StringUtils.isEmpty(seqno)) {
			result.setReCode(400);
			result.setReMsg("seqno不能为空");
			return result;
		}
		result.setReCode(200);
		result.setReMsg("执行成功");
		return result;
	}
	
	/**
	 * 失效处理
	 * 
	 * @param seqno
	 * @return
	 */
	@RequestMapping({ "/synCache/doInValid" })
	@ResponseBody
	public BaseResult invalidTask(String xmlid) {
		BaseResult result = new BaseResult();

		if (StringUtils.isEmpty(xmlid)) {
			result.setReCode(Integer.valueOf(400));
			result.setReMsg("xmlid不能为空");
			return result;
		}
		if (MemCache.PROC_MAP.containsKey(xmlid)) {
			MemCache.PROC_MAP.remove(xmlid);
			result.setReCode(Integer.valueOf(200));
			result.setReMsg("任务配置失效成功");
			return result;
		}

		if(MemCache.RUN_TASK.get(xmlid)==null||"".equals(MemCache.RUN_TASK.get(xmlid))) {
			MemCache.RUN_TASK.remove(xmlid);
		}
		result.setReCode(Integer.valueOf(400));
		result.setReMsg("任务为已失效状态");
		return result;
	}
}
