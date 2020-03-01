package com.asiainfo.dacp.scheduler.zk;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ai.zq.common.reg.base.ElectionCandidate;
import com.asiainfo.dacp.scheduler.service.TaskProcessService;


/**
 * 选举监听
 * @author zhangqi
 *
 */
@Component
public class ElectionCandidateListener implements ElectionCandidate{
	
	@Autowired
	private TaskProcessService tps;
	
	@Override
	public void startLeadership() throws Exception {
		// TODO Auto-generated method stub
		tps.setZkLeader(true);
	}

	@Override
	public void stopLeadership() {
		// TODO Auto-generated method stub
		tps.setZkLeader(false);
	}

}


