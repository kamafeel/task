package com.asiainfo.dacp.scheduler.event;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.asiainfo.dacp.dp.server.scheduler.bean.TaskConfig;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskLog;
import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbStorage;
import com.asiainfo.dacp.dp.tools.TimeUtils;
import com.asiainfo.dacp.scheduler.service.TaskService;
import com.lmax.disruptor.EventHandler;

/**
 * 运行超时处理
 * @author zhangqi
 *
 */
@Service
public class RunTimeOutHandler implements EventHandler<TaskLogEvent> {
	
	private Logger LOG = LoggerFactory.getLogger(RunTimeOutHandler.class);
	@Autowired
	private TaskEventRdbStorage storage;
	@Autowired
	private TaskService ts;
	
	@Override
	public void onEvent(TaskLogEvent event, long sequence, boolean endOfBatch) throws Exception {

		try {
			TaskLog tl = event.getTl();
			TaskConfig tc = MemCache.PROC_MAP.get(tl.getXmlid());
			Date now = new Date();
			Date end = tl.getExecTime()== null ? now : TimeUtils.string2Date(tl.getExecTime(), "yyyy-MM-dd HH:mm:ss");
			long timeDiff = now.getTime() - end.getTime();
			long waitHour = timeDiff / (1000 * 3600);
			if (waitHour >= tc.getMaxRunHours()) {
				LOG.warn("task [{}] will be kill,because it's timeout",tl.getTaskId());
				ts.killProcess(tl);
				storage.setTaskError2QueueOut(tl.getSeqno());
			}
		} catch (Exception e) {
			LOG.error("hard error,{}",event.getTl()== null ? "can't found taskLog" : event.getTl().getTaskId(),e);
		}
	}
}