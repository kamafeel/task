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
import com.lmax.disruptor.EventHandler;

/**
 * 失败任务处理
 * @author zhangqi
 *
 */
@Service
public class FailHandler implements EventHandler<TaskLogEvent> {
	private Logger LOG = LoggerFactory.getLogger(FailHandler.class);
	@Autowired
	private TaskEventRdbStorage storage;
	
	@Override
	public void onEvent(TaskLogEvent event, long sequence, boolean endOfBatch) throws Exception {

		try {
			TaskConfig config = MemCache.PROC_MAP.get(event.getTl().getXmlid());
			TaskLog tl = event.getTl();
			// 如果配置信息为空,直接出队
			if (config == null) {
				storage.setTaskQueueOut(tl.getSeqno());
			}
			// 已重做次数
			Integer retryNum = tl.getRetryNum();
			if (config.getRedoNum() > retryNum) {
				Date now = new Date();
				Date end = tl.getEndTime() == null ? now : TimeUtils.string2Date(tl.getEndTime(), "yyyy-MM-dd HH:mm:ss");
				long timeDiff = now.getTime() - end.getTime();
				long waitMinute = timeDiff / (1000 * 60);
				// 判断间隔时间和配置的重做间隔时间
				if (waitMinute >= config.getRedoInterval()) {
					storage.resetTask2RunModel(tl.getSeqno(), retryNum + 1);
				}
			} else {
				storage.setTaskQueueOut(tl.getSeqno());
			}
		} catch (Exception e) {
			LOG.error("hard error,{}",event.getTl()== null ? "can't found taskLog" : event.getTl().getTaskId(),e);
		}

	}
}