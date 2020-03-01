package com.asiainfo.dacp.scheduler.runnable;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.ai.zq.common.util.concurrent.BlockUtils;
import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;

/**
 * 数据库事务执行处理线程
 * @author zhangqi
 *
 */
@Service
public class DbExecute2TranThread implements Runnable{
	
	private Logger logger = LoggerFactory.getLogger(DbExecute2TranThread.class);
	@Resource(name="jdbcTemplate")
	private JdbcTemplate jdbcTemplate;
	@Value("${trandb.commit.maxnum}")
	private long maxNum;
	
	@Override
	public  void run() {
		DefaultTransactionDefinition def = new DefaultTransactionDefinition(TransactionDefinition.PROPAGATION_REQUIRED);
		DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(jdbcTemplate.getDataSource());
		while (true) {
			BlockUtils.waitingShortTime();
			List<String> sqlSet = getSqls();
			TransactionStatus status = null;
			if(sqlSet.isEmpty()){
				MemCache.DB_EXECUTE_QUEUE_MONITOR.put("NOW", System.currentTimeMillis());
				continue;
			}
			try {
				status = transactionManager.getTransaction(def);
				jdbcTemplate.batchUpdate(sqlSet.toArray(new String[sqlSet.size()]));
			} catch (Exception e) {
				transactionManager.rollback(status);
				logger.error("DataSourceTransactionManager is error:", e);
			} finally {
				if (status!=null && !status.isCompleted()) {
					transactionManager.commit(status);
				}
				status.flush();
				logger.info("SQL submit success");
				for(String sql:sqlSet) {
					MemCache.DB_EXECUTE_QUEUE_DICTIONARY.remove(sql);
				}
				MemCache.DB_EXECUTE_QUEUE_MONITOR.put("NOW", System.currentTimeMillis());
			}
		}
	}
	
	private List<String> getSqls(){
		List<String> sqls = new ArrayList<String>();
		int index=0;
		while(MemCache.DB_EXECUTE_QUEUE.peek()!=null){
			if(index<maxNum){
				sqls.add(MemCache.DB_EXECUTE_QUEUE.poll());
			}else{
				return sqls;
			}
			index++;
		}
		return sqls;
	}
}
