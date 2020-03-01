package com.asiainfo.dacp.scheduler.event;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.asiainfo.dacp.dp.server.scheduler.bean.MetaLog;
import com.asiainfo.dacp.dp.server.scheduler.bean.TargetObj;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskLog;
import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbStorage;
import com.asiainfo.dacp.dp.server.scheduler.type.DataFreq;
import com.asiainfo.dacp.dp.server.scheduler.type.ObjType;
import com.asiainfo.dacp.dp.tools.TimeUtils;
import com.asiainfo.dacp.scheduler.service.TaskService;
import com.lmax.disruptor.EventHandler;

/**
 * 任务触发
 * @author zhangqi
 *
 */
@Service
public class SucTriggerHandler implements EventHandler<TaskLogEvent> {
	
	private Logger LOG = LoggerFactory.getLogger(SucTriggerHandler.class);
	@Autowired
	private TaskEventRdbStorage storage;
	@Autowired
	private TaskService ts;
	
	@Override
	public void onEvent(TaskLogEvent event, long sequence, boolean endOfBatch) throws Exception {
		TaskLog tl = null;
		Map<String,TargetObj> mt = null;
		try {
			tl = event.getTl();
			this.refurbishRunModeEvent(tl);
			if(!MemCache.TARGET_MAP.containsKey(tl.getXmlid())){
				//出队
				storage.setTaskQueueOut(tl.getSeqno());
				return;
			}
			
			mt = new ConcurrentHashMap<String,TargetObj>();
			mt.putAll(MemCache.TARGET_MAP.get(tl.getXmlid()));
			//非月末,日末。剔除相应触发规则
			ts.nonLastTrigger(mt,tl.getDateArgs(),false);
			if(tl.getTriggerFlag()==1){
				//出队
				storage.setTaskQueueOut(tl.getSeqno());
				return;
			}
			
			//剔除时间类型任务
			ts.clearTriggerTimeTask(mt);
			if(mt == null || mt.isEmpty()){
				//出队
				storage.setTaskQueueOut(tl.getSeqno());
				return;
			}
			
			LOG.info("task[{}] have {} next tasks:", tl.getTaskId(), mt.size());
			Set<Map.Entry<String, TargetObj>> set = mt.entrySet();
			Iterator<Map.Entry<String, TargetObj>> it = set.iterator();
			while (it.hasNext()) {
				Map.Entry<String, TargetObj> item = it.next();
				TargetObj obj = item.getValue();				
				switch (ObjType.valueOf(obj.getTargettype())) {
				case PROC:
					TaskLog newtl = ts.createTaskRunInfo(MemCache.PROC_MAP.get(obj.getTarget()), null,
							TimeUtils.getProcTargetProcDateArgs(obj.getSourcefreq(),tl.getDateArgs()));
					LOG.info("task[{}] create next task:[{}]", tl.getTaskId(), 
							newtl == null? "TaskExist" : newtl.getTaskId());
					break;
				default:
					if(obj.getTargetfreq().equals(DataFreq.N.name())){
						break;
					}
					MetaLog ml = ts.push2Meta(tl,obj);
					LOG.info("task[{}] create next data:[{},{}]", new String[]{tl.getTaskId(), obj.getTarget(),ml.getDataTime()});
					//获取数据后续的程序
					if(ml.getTarget() == null || !MemCache.TARGET_MAP.containsKey(ml.getTarget()) || MemCache.TARGET_MAP.get(ml.getTarget()).isEmpty()){
						break;
					}
					Map<String,TargetObj> mtdata = new ConcurrentHashMap<String,TargetObj>();
					mtdata.putAll(MemCache.TARGET_MAP.get(ml.getTarget()));
					LOG.info("data[{}] have {} next tasks:", obj.getTarget(), mtdata.size());
					
					//非月末,日末。剔除相应触发规则
					ts.nonLastTrigger(mtdata,ml.getDataTime(),true);
					//剔除时间类型任务
					ts.clearTriggerTimeTask(mtdata);
					
					Set<Map.Entry<String, TargetObj>> setData = mtdata.entrySet();
					Iterator<Map.Entry<String, TargetObj>> itdata = setData.iterator();
					while (itdata.hasNext()) {
						Map.Entry<String, TargetObj> itemData = itdata.next();
						TargetObj objData = itemData.getValue();					
						switch (ObjType.valueOf(objData.getTargettype())) {
						case PROC:
							TaskLog dataNext = ts.createTaskRunInfo(MemCache.PROC_MAP.get(objData.getTarget()), null,
									TimeUtils.getDataTargetProcDateArgs(objData.getSourcefreq(),ml.getDataTime()));
							LOG.info("data[{},{}] create next task:[{}]", new String[]{ml.getTarget(),ml.getDataTime(), 
									dataNext == null? "TaskExist" : dataNext.getTaskId()});
							break;
						default:
							LOG.error("data[{},{}] can't create next data:[{}]", new String[]{ml.getTarget(),ml.getDataTime(),itemData.getKey()}); 
							break;
						}
					}
					break;
				}
			}		
			//出队
			storage.setTaskQueueOut(tl.getSeqno());
		} catch (Exception e) {
			LOG.error("hard error,{}",event.getTl()== null ? "can't found taskLog" : event.getTl().getTaskId(),e.getMessage());
		} finally {
			//刷新事件
			try {
				this.refurbishSoureEvent(tl,MemCache.TARGET_MAP.get(tl.getXmlid()));
			} catch (Exception e) {
				LOG.error("hard error,{}",event.getTl()== null ? "can't found taskLog" : event.getTl().getTaskId(),e.getMessage());
			}
		}
	}
	
	
	/**
	 * 刷新任务依赖事件
	 * @param mt
	 * @throws Exception 
	 */
	private void refurbishSoureEvent(TaskLog tl, Map<String,TargetObj> mt) throws Exception{
		if(tl == null || mt == null || mt.isEmpty()){
			return;
		}
		List<String> sucKeys = new ArrayList<String>();
		sucKeys.add(tl.getXmlid() + "@" + tl.getDateArgs());
		
		Set<Map.Entry<String, TargetObj>> set_tm = mt.entrySet();
		Iterator<Map.Entry<String, TargetObj>> it_tm = set_tm.iterator();
		while (it_tm.hasNext()) {
			Map.Entry<String, TargetObj> item = it_tm.next();
			TargetObj obj = item.getValue();				
			switch (ObjType.valueOf(obj.getTargettype())) {
			case PROC:
				break;
			default:
				if(!obj.getTargetfreq().equals(DataFreq.N.name())){
					sucKeys.add(obj.getTarget()+ "@" + TimeUtils.getProcTargetDataDataTime(obj.getTargetfreq(),tl.getDateArgs()));
				}				
				break;
			}
		}
		
		Set<Entry<String, Map<String, Integer>>> set = MemCache.SOURCE_EVENT.entrySet();
		Iterator<Entry<String, Map<String, Integer>>> it = set.iterator();
		while (it.hasNext()) {
			Entry<String, Map<String, Integer>> item = it.next();
			String soureKey = item.getKey();
			Set<Entry<String, Integer>> set_ = item.getValue().entrySet();
			Iterator<Entry<String, Integer>> it_ = set_.iterator();
			while (it_.hasNext()) {
				String key = it_.next().getKey();
				if(sucKeys.contains(key)){
					//String[] tmp = { key, soureKey, sucKeys.toString(), tl.getTaskId()};
					//LOG.info("depend [{}] of task[{}] trigger success by follow-up[{}] of task [{}]",tmp);
					MemCache.SOURCE_EVENT.get(soureKey).put(key, 1);
				}
			}
		}
//		for(String s : sucKeys){
//			MemCache.EVENT_SUC_FIFO.put(s, 1);
//		}

	}
	
	/**
	 * 刷新任务执行模型事件
	 * @param mt
	 * @throws Exception 
	 */
	private void refurbishRunModeEvent(TaskLog tl) throws Exception{
		String rmKey = tl.getXmlid()+ "@" + tl.getDateArgs();
		Set<Entry<String, Map<String, Integer>>> set = MemCache.RUNMODE_EVENT.entrySet();
		Iterator<Entry<String, Map<String, Integer>>> it = set.iterator();
		while (it.hasNext()) {
			Entry<String, Map<String, Integer>> item = it.next();
			String soureKey = item.getKey();
			Set<Entry<String, Integer>> set_ = item.getValue().entrySet();
			Iterator<Entry<String, Integer>> it_ = set_.iterator();
			while (it_.hasNext()) {
				if(it_.next().getKey().equals(rmKey)){
					MemCache.RUNMODE_EVENT.get(soureKey).put(rmKey, 1);					
				}
			}
		}
	}
}