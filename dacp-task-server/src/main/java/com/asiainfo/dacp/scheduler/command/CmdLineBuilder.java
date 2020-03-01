package com.asiainfo.dacp.scheduler.command;

import java.util.HashMap;

import com.asiainfo.dacp.dp.server.scheduler.bean.TaskLog;

/**
 * 命令拼装统一接口,各地个性化开发,请实现此接口
 * @author zhangqi
 *
 */
public interface CmdLineBuilder {
	HashMap<String, String> buildCmdLine(TaskLog tl) throws Exception;
}
