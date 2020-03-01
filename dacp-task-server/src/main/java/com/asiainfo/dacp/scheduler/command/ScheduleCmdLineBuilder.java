package com.asiainfo.dacp.scheduler.command;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.asiainfo.dacp.dp.message.MapKeys;
import com.asiainfo.dacp.dp.server.scheduler.bean.RunPara;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskConfig;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskGlobalVal;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskLog;
import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbSearch;
import com.asiainfo.dacp.dp.server.scheduler.type.RunFreq;
import com.asiainfo.dacp.dp.server.scheduler.type.ScriptType;
import com.asiainfo.dacp.dp.server.scheduler.utils.DpExecutorUtils;
import com.asiainfo.dacp.dp.tools.TimeUtils;

/**
 * 命令拼接处理类
 * @author zhangqi
 * TODO 需要优化
 */
public class ScheduleCmdLineBuilder implements CmdLineBuilder {
	
	private Logger LOG = LoggerFactory.getLogger(ScheduleCmdLineBuilder.class);
	
	@Autowired
	private TaskEventRdbSearch search;

	public HashMap<String, String> buildCmdLine(TaskLog runInfo) throws Exception {
		HashMap<String, String> map = new HashMap<String, String>();
		String cmdLine = null;
		String path = null;
		String dateArgs = runInfo.getDateArgs().replaceAll("-+|\\s+|:+", "");//标准化处理时间格式 参数的时间格式为yyyyMMddHHmmss
		List<RunPara> lrps = search.queryRunpara(runInfo.getXmlid());
		String runpara = this.getRunpara(lrps, runInfo, dateArgs);
		if(!lrps.isEmpty()){
			runpara = DpExecutorUtils.variableSubstitution(runpara, this.getTemplate(dateArgs,runInfo.getDateArgs()));
		}
		//如果参数列表为空，则默认追加日期批次
		if (StringUtils.isEmpty(runpara)){			
			runpara = dateArgs;
		}
		TaskConfig task = MemCache.PROC_MAP.get(runInfo.getXmlid());
		//手工任务
		if (runInfo.getRunFreq().equals(RunFreq.manual.name())) {
			path = runInfo.getPath();
			cmdLine = new StringBuilder().append(runInfo.getPreCmd()).append(" ").append(path).append(runpara).toString();
		} else {
			//如果是dp程序,下面是一段如此垃圾的代码,暂时不想优化
			if (StringUtils.equalsIgnoreCase(runInfo.getProctype(), ScriptType.dp.name())) {
				// 程序路径
				path = (task == null || StringUtils.isEmpty(task.getPath())) ? " " : task.getPath();
				cmdLine = new StringBuilder()
						.append(StringUtils.isEmpty(runInfo.getPreCmd()) ? "sh " : runInfo.getPreCmd() + " ")
						.append(path).append(" ").append(runpara).toString();
			} else {
				path = (task == null || StringUtils.isEmpty(task.getPath())) ? " "
						: (task.getPath().endsWith("/") ? task.getPath() : task.getPath() + "/");
				cmdLine = new StringBuilder().append(runInfo.getPreCmd()).append(" ").append(path)
						.append((task == null || StringUtils.isEmpty(task.getExecProc())) ? runInfo.getProcName()
								: task.getExecProc())
						.append(" ").append(runpara).toString();
			}
		}

		String charSet = null;
		if (task != null && task.getCharacterSet() != null) {
			charSet = MapKeys.LogFileCharacterSet.getValue(task.getCharacterSet());
		}
		if (charSet == null) {
			charSet = MapKeys.LogFileCharacterSet.UTF8.getValue();
		}

		map.put(MapKeys.CMD_PARA, cmdLine);
		map.put(MapKeys.CMD_PRE, "sh");
		map.put(MapKeys.CMD_NAME, "dacp_task_proxy.sh");
		map.put(MapKeys.LOG_FILE_CHARACTER_SET, charSet);
		return map;
	}
	
	/**
	 * 处理调用命令参数
	 * @param list
	 * @param tl
	 * @param dateArgs
	 * @return
	 */
	private String getRunpara(List<RunPara> list,TaskLog tl,String dateArgs){
		StringBuilder rpStr = new StringBuilder();
		String key = tl.getXmlid()+"@"+tl.getDateArgs();
		int rerurnCode = 0;
		if(MemCache.REDO_FROM_ERRORCODE.containsKey(key)){
			rerurnCode = MemCache.REDO_FROM_ERRORCODE.get(key);
			MemCache.REDO_FROM_ERRORCODE.remove(key);
		}
		//DP程序特殊处理
		if (tl.getProctype().equalsIgnoreCase(ScriptType.dp.name())) {
			String execProc = MemCache.PROC_MAP.get(tl.getXmlid()).getExecProc();
			execProc = execProc == null || execProc.trim().equals("")
					? MemCache.PROC_MAP.get(tl.getXmlid()).getProcName() : execProc;
			rpStr.append(" -t ").append(dateArgs).append(" -f ").append(execProc).append(" -i ").append(rerurnCode);
		}		
		for(RunPara rp : list){
			rpStr.append(" ").append(rp.getRunPara()).append(" ").append(rp.getRunParaValue());
		}	
		return rpStr.toString();
	}
	
	/**
	 * 获取转换模板
	 * @param dateArgs
	 * @return
	 * @throws NumberFormatException
	 * @throws Exception
	 */
	private Map<String, String> getTemplate(String dateArgs,String sDateArgs) throws NumberFormatException, Exception{
		Map<String, String> template = new HashMap<String, String>();
		List<TaskGlobalVal> varList = search.queryGlobalVal();
		Pattern p = Pattern.compile("\\$\\{(.*?)\\}");
		for(TaskGlobalVal tgl : varList){
			if(StringUtils.equalsIgnoreCase(tgl.getVarType(), MapKeys.VARTYPE.CONST.name())){
				Matcher m = p.matcher(tgl.getVarValue());
				m.find();
				String str = m.group(1);
				if (!StringUtils.isEmpty(str)) {
					String[] values = str.split("\\|");
					try {
						template.put(tgl.getVarName(), TimeUtils.date2String(this.getValDate(values[0], Integer.parseInt(values[2]), dateArgs), values[1]));
					} catch (Exception e) {
						LOG.error("create template is fail", e);
					}
				}
			//固定参数
			}else if(StringUtils.equalsIgnoreCase(tgl.getVarType(), MapKeys.VARTYPE.PRPAM.name())){
				template.put(tgl.getVarName(), tgl.getVarValue());
			}else{
				LOG.warn("varType [{}] can't be process ",tgl.getVarType());
			}
		}
		//固定默认参数
		template.put("taskid", dateArgs);
		template.put("dateArgs", sDateArgs);
		return template;
	}
	
	/**
	 * 获取参数时间
	 * @param type
	 * @param offset
	 * @param dateArgs
	 * @return
	 * @throws Exception
	 */
	private Date getValDate(String type, int offset, String dateArgs) throws Exception {
		Calendar ca = Calendar.getInstance();
		ca.setTime(TimeUtils.string2Date(StringUtils.rightPad(dateArgs, 14, "0"), "yyyyMMddHHmmss"));
		switch (type) {
		case "day":
			ca.add(Calendar.DATE, offset);
			return ca.getTime();
		case "month":
			ca.add(Calendar.MONTH, offset);
			return ca.getTime();
		case "hour":
			ca.add(Calendar.HOUR, offset);
			return ca.getTime();
		case "minute":
			ca.add(Calendar.MINUTE, offset);
			return ca.getTime();
		case "year":
			ca.add(Calendar.YEAR, offset);
			return ca.getTime();
		default:
			throw new Exception(type + " val cycle type can't be process!");
		}
	}
}
