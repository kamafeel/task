package com.asiainfo.dacp.scheduler.event.personality;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.asiainfo.dacp.dp.server.scheduler.bean.MetaLog;
import com.asiainfo.dacp.dp.server.scheduler.bean.TargetObj;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskLog;
import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbSearch;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbStorage;
import com.asiainfo.dacp.dp.server.scheduler.type.DataFreq;
import com.asiainfo.dacp.dp.server.scheduler.type.ObjType;
import com.asiainfo.dacp.dp.tools.TimeUtils;
import com.asiainfo.dacp.scheduler.event.TaskLogEvent;
import com.asiainfo.dacp.scheduler.service.TaskService;
import com.lmax.disruptor.EventHandler;

/**
 * 重庆任务成功后触发个性化操作
 * 
 * @author zhangqi
 *
 */
public class CQ_SucTaskHandler implements EventHandler<TaskLogEvent> {

	private Logger LOG = LoggerFactory.getLogger(CQ_SucTaskHandler.class);
	@Autowired
	private TaskService ts;
	@Autowired
	private TaskEventRdbSearch search;
	@Autowired
	private TaskEventRdbStorage storage;

	@Override
	public void onEvent(TaskLogEvent event, long sequence, boolean endOfBatch) throws Exception {
		// TODO Auto-generated method stub
		try {
			Thread.sleep(999999);
			TaskLog tl = event.getTl();
			if (tl.getTriggerFlag() == 1) {
				return;
			}
			if (!MemCache.TARGET_MAP.containsKey(tl.getXmlid())) {
				return;
			}
			Map<String, TargetObj> mt = new ConcurrentHashMap<String, TargetObj>();
			mt.putAll(MemCache.TARGET_MAP.get(tl.getXmlid()));
			// 非月末,日末。剔除相应触发规则
			ts.nonLastTrigger(mt, tl.getDateArgs(), false);

			Set<Map.Entry<String, TargetObj>> set = mt.entrySet();
			Iterator<Map.Entry<String, TargetObj>> it = set.iterator();
			List<MetaLog> list = new ArrayList<MetaLog>();

			while (it.hasNext()) {
				Map.Entry<String, TargetObj> item = it.next();
				TargetObj obj = item.getValue();
				switch (ObjType.valueOf(obj.getTargettype())) {
				case DATA:
					if (obj.getTargetfreq().equals(DataFreq.N.name())) {
						break;
					}
					list.add(ts.getMeta(tl, obj));
					break;
				default:
					break;
				}
			}
			ts.pushMessage(list);

			// 扫描事件
			int cnt = 0;
			List<MetaLog> insertList = new ArrayList<MetaLog>();
			String finalSql = "SELECT 1 FROM proc_schedule_meta_log WHERE 1=1 ";
			String realSql = "";
			String target = "";
			String data_time = "";
			for (MetaLog metaLog : list) {
				target = metaLog.getTarget();
				data_time = metaLog.getDataTime();
				realSql = finalSql + " and target='" + target + "' ";
				if (data_time.length() == 10) {// 小时到日
					cnt = 24;
					realSql += " AND data_time LIKE '" + data_time.substring(0, 8) + "__" + "'"
							+ " HAVING COUNT(target)=" + cnt;
					if (search.checkExist(realSql) == 1) {
						MetaLog mLog = metaLog.clone();
						if (mLog != null) {
							mLog.setDataTime(data_time.substring(0, 8));
							mLog.setTriggerFlag(0);
							insertList.add(mLog);
							LOG.info("DATA [{}] scanEvent [{}]", mLog.getTarget(), mLog.getDataTime());
						}
					}
				} else if (data_time.length() == 8) {// 日到月
					if (TimeUtils.isLast("day", data_time, true)) {
						MetaLog mLog = metaLog.clone();
						if (mLog != null) {
							mLog.setDataTime(data_time.substring(0, 6));//月时间格式为yyyyMM
							mLog.setTriggerFlag(0);
							insertList.add(mLog);
							LOG.info("DATA [{}] scanEvent [{}]", mLog.getTarget(), mLog.getDataTime());
						}
					}
				} else {
					continue;
				}
			}
			if (!insertList.isEmpty()) {
				storage.insertTargertLog(insertList);
			}
		} catch (Exception e) {
			LOG.error("hard error,{}", event.getTl() == null ? "can't found taskLog" : event.getTl().getTaskId(), e);
		}
	}

}
