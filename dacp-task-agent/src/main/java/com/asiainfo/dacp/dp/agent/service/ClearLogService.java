package com.asiainfo.dacp.dp.agent.service;

import java.util.Map;

import org.springframework.stereotype.Service;
import com.asiainfo.dacp.dp.agent.DpAgentContext;
import com.asiainfo.dacp.dp.message.MapKeys;
import com.asiainfo.dacp.dp.message.DpMessage;
import com.asiainfo.dacp.process.DpProcess;

import org.springframework.beans.factory.annotation.Autowired;

@Service
public class ClearLogService {
	@Autowired
	private DpProcess process;
	
	public String service(DpMessage msg,DpAgentContext context){
		Map<String, String> firstMap = msg.getFirstMap();
		String date = firstMap.get(MapKeys.PROC_DATE_VAR);
		boolean isOK = false;
		try {
			Process proc= process.createProcess(new String[]{"./sbin/rmlog.sh " + date,context.getLogPath()});
			isOK = proc.waitFor() == 0;
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return String.valueOf(isOK);
	}
}
