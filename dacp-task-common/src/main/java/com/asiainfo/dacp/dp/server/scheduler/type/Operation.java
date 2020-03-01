package com.asiainfo.dacp.dp.server.scheduler.type;

public enum Operation {	
	redoAfter,//重做后续
	redoCur,//重做当前
	forceExec,//强制执行
	forcePass,//强制通过
	setPriLevel,//设置优先级
	pauseTask,//暂停任务
	recoverTask,//恢复任务
	manualTask,//手工任务
	executeManual,//手动执行
	kill,//KILL任务
	getWaitCode,//排队等待原因
	getCondiNotrigger,//获取未触发依赖条件
	executionWithoutDelay,//立刻执行
	addRunLogs, //手动增加任务运行信息
	getFlowInfo, //获取流程图信息
	analysisNotriggerReason //分析未触发原因
}
