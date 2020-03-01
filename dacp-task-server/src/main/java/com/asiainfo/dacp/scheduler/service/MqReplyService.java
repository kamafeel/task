package com.asiainfo.dacp.scheduler.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.asiainfo.dacp.dp.message.MapKeys;
import com.asiainfo.dacp.dp.common.RunStatus;
import com.asiainfo.dacp.dp.message.DpHandler;
import com.asiainfo.dacp.dp.message.DpMessage;
import com.asiainfo.dacp.dp.server.scheduler.bean.MetaLog;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskLog;
import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbSearch;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbStorage;
import com.asiainfo.dacp.dp.message.MsgType;
import com.asiainfo.dacp.dp.server.scheduler.type.RunFreq;
import com.google.gson.Gson;;

/**
 * 接收消息服务
 * @author Silence
 *
 */
public class MqReplyService implements DpHandler {
	
	@Autowired
	private TaskEventRdbSearch search;
	@Autowired
	private TaskEventRdbStorage storage;
	@Autowired
	private TaskService ts;
	
	private static Logger LOG = LoggerFactory.getLogger(MqReplyService.class);
	
	/*** 任务处理 **/
	public Object onMessage(Object _msgObj) {
		this.processMsg(_msgObj);
		return null;
	}
	
	private void processMsg(Object _msgObj){
		try {
			if (_msgObj == null) {
				LOG.error("surprise,this msgObj is null");
				return;
			}
			DpMessage msgObj = (DpMessage) _msgObj;
			LOG.debug("server receive mq message:{}",new Gson().toJson(msgObj));
			Map<String, String> reMap = msgObj.getFirstMap();
			if (reMap == null) {
				return;
			}
			String msgType = msgObj.getMsgType();
			MsgType _msgType = MsgType.valueOf(msgType);
			switch (_msgType) {
			case taskTypeFunc:// 平台程序
				String seqno = msgObj.getMsgId();
				String _status = reMap.get(MapKeys.PROC_STATUS);
				if (_status == null) {
					LOG.error("the status of task[{}] was null,pass this msg", seqno);
					return;
				}
				int status = Integer.parseInt(_status);
				TaskLog runInfo = search.queryTaskRunLog(seqno);
				if (runInfo == null) {
					LOG.error("task[{}] was deleted or ignore,pass this msg", seqno);
					return;
				}
				if ((int) runInfo.getQueueFlag() == 1) {
					LOG.error("task[{}] was dequeued, the result is invalid,pass this msg", runInfo.getTaskId());
					return;
				}
				if (status == RunStatus.PROC_RUNNING) {
					TaskCacheService.addRunTaskTemp(runInfo.getXmlid(), runInfo.getDateArgs());
					LOG.info("task[{}] is running! ", runInfo.getTaskId());
					storage.updateTaskInRuning(seqno,reMap.get(MapKeys.PROC_PID));
				} else if (status == RunStatus.PROC_RUN_SUCCESS) {
					TaskCacheService.removeRunTaskTemp(runInfo.getXmlid(), runInfo.getDateArgs());
					TaskCacheService.removeAgentRunning(runInfo.getAgentCode(),runInfo.getSeqno());
					LOG.info("task[{}] run success! ", runInfo.getTaskId());
					if (StringUtils.equals(runInfo.getRunFreq(), RunFreq.manual.name())) {// 处理成功手工任务
						storage.updateTaskRunFinish(seqno, RunStatus.PROC_RUN_SUCCESS, 0, 1, 1, runInfo.getExecTime());
					} else{
						storage.updateTaskRunFinish(seqno, RunStatus.PROC_RUN_SUCCESS, 0, 0, 0, runInfo.getExecTime());
					}
					if(ts.isRunLog2db()){
						storage.insertScriptLog(seqno, reMap.get(MapKeys.PROC_LOG));
					}
				} else {
//					MemCache.RUN_TASK.remove(runInfo.getXmlid());
					TaskCacheService.removeRunTaskTemp(runInfo.getXmlid(), runInfo.getDateArgs());
					TaskCacheService.removeAgentRunning(runInfo.getAgentCode(),runInfo.getSeqno());
					LOG.info("task[{}] run fail!", runInfo.getTaskId());
					if (StringUtils.equals(runInfo.getRunFreq(), RunFreq.manual.name())) {// 处理失败手工任务
						storage.updateTaskRunFinish(seqno, RunStatus.PROC_RUN_FAIL, StringUtils.isEmpty(reMap.get(MapKeys.PROC_RETURN_CODE)) ? 0
								: Integer.valueOf(reMap.get(MapKeys.PROC_RETURN_CODE)), 1, 1, runInfo.getExecTime());
					}else{
						storage.updateTaskRunFinish(seqno, RunStatus.PROC_RUN_FAIL, StringUtils.isEmpty(reMap.get(MapKeys.PROC_RETURN_CODE)) ? 0
								: Integer.valueOf(reMap.get(MapKeys.PROC_RETURN_CODE)), 0, 0, runInfo.getExecTime());
					}
					if(ts.isRunLog2db()){
						LOG.info("content-length {}", reMap.get(MapKeys.PROC_LOG).length());
						storage.insertScriptLog(seqno, reMap.get(MapKeys.PROC_LOG));
					}
				}
				break;
			case KILL_PROC:
				String kill_seqno = msgObj.getMsgId();												
				LOG.info("task[{}] was kill", kill_seqno);
				TaskLog kill_runInfo = search.queryTaskRunLog(kill_seqno);
				if (kill_runInfo == null) {
					LOG.error("task[{}] was deleted or ignore,pass this msg", kill_seqno);
					return;
				}
//				MemCache.RUN_TASK.remove(kill_runInfo.getXmlid());
				TaskCacheService.removeAgentRunning(kill_runInfo.getAgentCode(),kill_runInfo.getSeqno());
				TaskCacheService.removeRunTaskTemp(kill_runInfo.getXmlid(), kill_runInfo.getDateArgs());
				storage.updateTaskRunFinish(kill_seqno, RunStatus.PROC_RUN_FAIL, StringUtils.isEmpty(reMap.get(MapKeys.PROC_RETURN_CODE)) ? 0
						: Integer.valueOf(reMap.get(MapKeys.PROC_RETURN_CODE)), 1, 1,null);						
				break;
			case INTER://割接数据或者接口数据
				
				break;
			case RESAER_FLAG://重庆使用
				Map<String, String> dataMap = msgObj.getFirstMap();
				ts.resetDataTime(dataMap);
				if (!dataMap.isEmpty()) {
					MetaLog targetLog = ts.convertToMetaLog(dataMap);
					List<MetaLog> targetLogList = new ArrayList<MetaLog>();
					targetLogList.add(targetLog);						
					if (StringUtils.isEmpty(targetLog.getProcName())) {
						storage.insertMetaLog(targetLog);
					} else {
						storage.insertTargertLog(targetLogList);
					}
					ts.pushMessage(targetLogList);
				}
				break;
			default:
				break;
			}
		} catch (Exception e) {
			LOG.error("hard error", e);
		}
	}
}