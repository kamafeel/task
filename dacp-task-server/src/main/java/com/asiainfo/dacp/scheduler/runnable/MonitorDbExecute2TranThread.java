package com.asiainfo.dacp.scheduler.runnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.ai.zq.common.util.concurrent.BlockUtils;
import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.dp.tools.TimeUtils;

/**
 * 监控异步提交效率
 * @author zhangqi
 *
 */
@Service
public class MonitorDbExecute2TranThread implements Runnable{
	
	private Logger logger = LoggerFactory.getLogger(MonitorDbExecute2TranThread.class);	
	@Value("${monitor.dbtran.timeout}")
	private long timeout;
	@Value("${monitor.dbtran.maxNum}")
	private long maxNum;
	
	@Override
	public  void run() {
		while(true){
			long end = System.currentTimeMillis();			
			if(MemCache.DB_EXECUTE_QUEUE_MONITOR.containsKey("NOW")){
				long start = MemCache.DB_EXECUTE_QUEUE_MONITOR.get("NOW");					
				long diff = (end - start) / 1000;
				int dtq = MemCache.DB_EXECUTE_QUEUE.size();
				if(diff > timeout){
					logger.warn("DbExecute2Tran is too slow:{} current size is {}",TimeUtils.formatLong(end-start),dtq);
					logger.warn("All SQL QUEUE",MemCache.DB_EXECUTE_QUEUE_DICTIONARY);
				}else if(dtq > maxNum){
					logger.warn("DbExecute2Tran is too much:{}",dtq);
					logger.warn("All SQL QUEUE",MemCache.DB_EXECUTE_QUEUE_DICTIONARY);
				}
			}
			BlockUtils.sleep(5000l);
		}
	}
}
