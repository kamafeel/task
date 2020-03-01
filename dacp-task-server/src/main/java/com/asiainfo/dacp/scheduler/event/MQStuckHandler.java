package com.asiainfo.dacp.scheduler.event;

import java.util.Date;

import org.apache.commons.lang3.StringUtils;
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
 * 发送MQ状态卡顿处理
 * @author zhangqi
 *
 */
@Service
public class MQStuckHandler implements EventHandler<TaskLogEvent> {
	
	@Autowired
	private TaskEventRdbStorage storage;
	@Autowired
	private TaskService ts;
	private Logger LOG = LoggerFactory.getLogger(MQStuckHandler.class);
	
	@Override
	public void onEvent(TaskLogEvent event, long sequence, boolean endOfBatch) throws Exception {
		
		try {
			TaskConfig config = MemCache.PROC_MAP.get(event.getTl().getXmlid());
			TaskLog tl = event.getTl();
			// 如果配置信息为空,直接出队
			if (config == null) {
				storage.setTaskQueueOut(tl.getSeqno());
			}
			if(ts.getMqStuck()==1){
				Date now = new Date();
				if(StringUtils.isNotEmpty(tl.getStatusTime())){
					long timeDiff = now.getTime() - TimeUtils.string2Date(tl.getStatusTime(), "yyyy-MM-dd HH:mm:ss").getTime();
					long waitSecond = timeDiff / 1000;
					// 判断间隔时间和配置的重做间隔时间
					if (waitSecond >= ts.getMqStuckTimeout()) {
						LOG.warn("task [{}] will be reSend,because it's send to mq timeout",tl.getTaskId());
						storage.resetTask2Send2MQ(tl.getSeqno(),StringUtils.isEmpty(config.getAgentCode()));
					}
				}			
			}
		} catch (Exception e) {
			LOG.error("hard error,{}",event.getTl()== null ? "can't found taskLog" : event.getTl().getTaskId(),e);
		}	
	}
}