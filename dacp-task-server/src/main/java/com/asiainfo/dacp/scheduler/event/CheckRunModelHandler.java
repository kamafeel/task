package com.asiainfo.dacp.scheduler.event;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.quartz.CronExpression;
import org.quartz.TriggerUtils;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.asiainfo.dacp.dp.common.RunStatus;
import com.asiainfo.dacp.dp.common.RunStatus.IsValid;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskConfig;
import com.asiainfo.dacp.dp.server.scheduler.bean.TaskLog;
import com.asiainfo.dacp.dp.server.scheduler.cache.MemCache;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbSearch;
import com.asiainfo.dacp.dp.server.scheduler.dao.TaskEventRdbStorage;
import com.asiainfo.dacp.dp.server.scheduler.type.RunFreq;
import com.lmax.disruptor.EventHandler;
import com.asiainfo.dacp.dp.server.scheduler.type.Type;
import com.asiainfo.dacp.dp.tools.TimeUtils;
import com.asiainfo.dacp.scheduler.service.TaskCacheService;

/**
 * 执行模型检测
 * @author zhangqi
 *
 */
@Service
public class CheckRunModelHandler implements EventHandler<TaskLogEvent> {
	
	private Logger LOG = LoggerFactory.getLogger(CheckRunModelHandler.class);
	
	@Autowired
	private TaskEventRdbSearch search;
	
	@Autowired
	private TaskEventRdbStorage storage;
	
	@Override
	public void onEvent(TaskLogEvent event, long sequence, boolean endOfBatch) throws Exception {
		try {
			TaskLog tl = event.getTl();
			TaskCacheService.removeRunTaskTemp(tl.getXmlid(),tl.getDateArgs());
			//检查是否人为限制任务不准运行
			if(isManualStop(tl.getDateArgs(),tl.getRunFreq())){
				MemCache.TASK_WAIT_REASON.put(tl.getSeqno(), RunStatus.ERRCODE.IS_ALLOW);
				return;
			}
			//检测同任务名，同批次号程序
			if(isRuningSameAll(tl.getXmlid(),tl.getDateArgs())){
				MemCache.TASK_WAIT_REASON.put(tl.getSeqno(), RunStatus.ERRCODE.SAME);
				return;
			}
			
			TaskConfig tc = MemCache.PROC_MAP.get(tl.getXmlid());
			int mutiRun = tc.getMutiRunFlag();
			//单一启动,并且要求顺序运行
			if (mutiRun == Type.RUN_TYPE.EXECUTE_SERIAL.ordinal()) {
				if (isRuningSameName(tl.getXmlid())){
					MemCache.TASK_WAIT_REASON.put(tl.getSeqno(), RunStatus.ERRCODE.MUT);
					return;
				}
				//如果上一批次未运行成功，当前批次不运行
				if(!isPreTaskRunSuc(tl.getXmlid(),tl.getRunFreq(),tc.getCronExp(),tc.getTriggerType() == 0,false,tl.getDateArgs())){
					MemCache.TASK_WAIT_REASON.put(tl.getSeqno(), RunStatus.ERRCODE.PRE);
					return;
				}
			//单一启动
			}else if(mutiRun == Type.RUN_TYPE.EXECUTE_ONECE.ordinal()){
				if (isRuningSameName(tl.getXmlid())){
					MemCache.TASK_WAIT_REASON.put(tl.getSeqno(), RunStatus.ERRCODE.MUT);
					return;
				}
			//周期内顺序启动
			}else if (mutiRun == Type.RUN_TYPE.EXECUTE_SERIAL_IN_CYCLE.ordinal()) {
				//如果周期内上一批次未运行，当前批次不运行
				if(!isPreTaskRunSuc(tl.getXmlid(),tl.getRunFreq(),tc.getCronExp(),tc.getTriggerType() == 0,true,tl.getDateArgs())){
					MemCache.TASK_WAIT_REASON.put(tl.getSeqno(), RunStatus.ERRCODE.CYCLE_PRE);
					return;
				}
			}
			
			LOG.info("task[{}] check runmodel success",tl.getTaskId());
			storage.updateTaskState(tl.getSeqno(), RunStatus.CHECK_RUNMODEL_SUCCESS);
			TaskCacheService.addRunTaskTemp(tl.getXmlid(),tl.getDateArgs());
		} catch (Exception e) {
			LOG.error("hard error,{}",event.getTl()== null ? "can't found taskLog" : event.getTl().getTaskId()+e.getMessage());
		}
	}

	private boolean isRuningSameAll(String xmlid,String dateArgs) {
		if(MemCache.RUN_TASK.get(xmlid)!=null&&MemCache.RUN_TASK.get(xmlid).contains(dateArgs)) {
			LOG.debug("task [{},{}], is running same all!!, running info [{}] ",xmlid,dateArgs,MemCache.RUN_TASK.get(xmlid));
			return true;
		}
//		if(MemCache.RUN_TASK.containsKey(xmlid) && MemCache.RUN_TASK.containsValue(dateArgs)){
//			return true;
//		}
		return false;
	}
	
	private boolean isRuningSameName(String xmlid) {
		if(MemCache.RUN_TASK.containsKey(xmlid)&&!"".equals(MemCache.RUN_TASK.get(xmlid))){
			LOG.debug("task [{}], is running same name!!, running info [{}] ",xmlid,MemCache.RUN_TASK.get(xmlid));
			return true;
		}
		return false;
	}
	
	
	private boolean isManualStop(String dateArgs,String runFreq) throws Exception{
		if(MemCache.MANUAL_STOP.isEmpty()){
			return false;
		}
		if (StringUtils.equalsIgnoreCase(runFreq, RunFreq.minute.name())
				|| StringUtils.equalsIgnoreCase(runFreq, RunFreq.hour.name())) {
			dateArgs = dateArgs.substring(0, 10);
		}
		
		if(MemCache.MANUAL_STOP.containsKey(dateArgs)){
			return true;
		}
		return false;
	}
	
	
	public static Date getConExpNearPastFireTimeMinute(String cronExp,Date date) {
			if (cronExp == null || "".equals(cronExp)) {
				return null;
			}
			CronTriggerImpl cronTriggerImpl = new CronTriggerImpl();
			try {
				cronTriggerImpl.setCronExpression(cronExp);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
			//作为开始时间
			Calendar originalbatch = Calendar.getInstance();
			Calendar calendar = Calendar.getInstance();
			Calendar original = Calendar.getInstance();
				originalbatch.setTime(date);
				originalbatch.add(Calendar.MINUTE, 0);
				calendar.setTime(originalbatch.getTime());
				original.setTime(originalbatch.getTime());
				original.add(Calendar.MILLISECOND, -1);
				calendar.add(Calendar.HOUR_OF_DAY, -1);
				
			//calendar.add(Calendar.MONTH, -2); 效率太慢
			List<Date> dates = TriggerUtils.computeFireTimesBetween(cronTriggerImpl, null, calendar.getTime(), original.getTime());
			if(dates.isEmpty()){
				return null;
			}
			return dates.get(dates.size()-1);
	}
	/**
	 * 获取时间触发表达式,上一个触发周期(date_args)
	 * @param cronExp
	 * @param proc_date
	 * @param runFreq
	 * @return
	 * @throws Exception 
	 */
	public String getCronExpLastFireTime(String cronExp, String dateArgs,String runFreq,int dateOffSet) throws Exception {
		if (cronExp == null || "".equals(cronExp)) {
			return null;
		}
		String returnStr = null;
		if (CronExpression.isValidExpression(cronExp)) {
			CronTriggerImpl cronTriggerImpl = new CronTriggerImpl();
			cronTriggerImpl.setCronExpression(cronExp);
			//作为开始时间
			Calendar originalbatch = Calendar.getInstance();
			Calendar calendar = Calendar.getInstance();
			Calendar original = Calendar.getInstance();
			String dateFormat = null;
			switch(runFreq){
			case "hour":
				originalbatch.setTime(TimeUtils.string2Date(dateArgs,"yyyy-MM-dd HH"));
				originalbatch.add(Calendar.HOUR, dateOffSet);
				calendar.setTime(originalbatch.getTime());
				original.setTime(originalbatch.getTime());				
				original.add(Calendar.MILLISECOND, -1);
				dateFormat = "yyyy-MM-dd HH";
				calendar.add(Calendar.DAY_OF_MONTH, -1);
				break;
			case "day":
				originalbatch.setTime(TimeUtils.string2Date(dateArgs,"yyyy-MM-dd"));
				originalbatch.add(Calendar.DAY_OF_MONTH, dateOffSet);
				calendar.setTime(originalbatch.getTime());
				original.setTime(originalbatch.getTime());
				original.add(Calendar.MILLISECOND, -1);
				dateFormat = "yyyy-MM-dd";
				calendar.add(Calendar.MONTH, -1);
				break;
			case "month":
				originalbatch.setTime(TimeUtils.string2Date(dateArgs,"yyyy-MM-dd"));
				originalbatch.add(Calendar.MONTH, dateOffSet);
				calendar.setTime(originalbatch.getTime());
				original.setTime(originalbatch.getTime());
				original.add(Calendar.MILLISECOND, -1);
				dateFormat = "yyyy-MM-01";
				calendar.add(Calendar.YEAR, -1);
				break;
			case "minute":
				originalbatch.setTime(TimeUtils.string2Date(dateArgs,"yyyy-MM-dd HH:mm"));
				originalbatch.add(Calendar.MINUTE, dateOffSet);
				calendar.setTime(originalbatch.getTime());
				original.setTime(originalbatch.getTime());
				original.add(Calendar.MILLISECOND, -1);
				dateFormat = "yyyy-MM-dd HH:mm";
				calendar.add(Calendar.HOUR_OF_DAY, -1);
				break;
			default:
				throw new Exception(runFreq + " RunFreq can't be process!");
			}
			//calendar.add(Calendar.MONTH, -2); 效率太慢
			List<Date> dates = TriggerUtils.computeFireTimesBetween(cronTriggerImpl, null, calendar.getTime(), original.getTime());
			if(dates.isEmpty()){
				return null;
			}
			originalbatch.setTime(dates.get(dates.size()-1));
			switch(runFreq){
			case "hour":
				originalbatch.add(Calendar.HOUR, -dateOffSet);				
				returnStr = TimeUtils.date2String(originalbatch.getTime(), dateFormat);
				break;
			case "day":
				originalbatch.add(Calendar.DAY_OF_MONTH, -dateOffSet);				
				returnStr = TimeUtils.date2String(originalbatch.getTime(), dateFormat);
				break;
			case "month":
				originalbatch.add(Calendar.MONTH, -dateOffSet);				
				returnStr = TimeUtils.date2String(originalbatch.getTime(), dateFormat);
				break;
			case "minute":
				originalbatch.add(Calendar.MINUTE, -dateOffSet);				
				returnStr = TimeUtils.date2String(originalbatch.getTime(), dateFormat);
				break;
			default:
				throw new Exception(runFreq + " RunFreq can't be process!");
			}
			return returnStr;
		}
		return null;
	}
	
	 
	
	/**
	 * 事件任务上一批次周期计算(暂时限定为当前周期-1)
	 * @param runFreq
	 * @param curDateArgs
	 * @return
	 * @throws Exception 
	 */
	private String getPreDateArgsForEventTask(String runFreq, String dateArgs) throws Exception {
		Calendar ca = Calendar.getInstance();		
		switch (RunFreq.valueOf(runFreq)) {
		case minute:
			ca.setTime(TimeUtils.string2Date(dateArgs,"yyyy-MM-dd HH:mm"));
			ca.add(Calendar.MINUTE, -1);
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd HH:mm");
		case hour:
			ca.setTime(TimeUtils.string2Date(dateArgs,"yyyy-MM-dd HH"));
			ca.add(Calendar.HOUR, -1);
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd HH");
		case day:
			ca.setTime(TimeUtils.string2Date(dateArgs,"yyyy-MM-dd"));
			ca.add(Calendar.DAY_OF_MONTH, -1);
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd");
		case month:
			ca.setTime(TimeUtils.string2Date(dateArgs,"yyyy-MM-dd"));
			ca.add(Calendar.MONTH, -1);
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd");
		default:
			throw new Exception(runFreq + " RunFreq can't be process!");
		}
		
	}
	
	/**
	 * 是否已跨越周期
	 * @return
	 * @throws Exception 
	 */
	private boolean overCycle(String preDateArgs,String dateArgs,String runFreq) throws Exception{
		boolean isOver = false;
		switch (RunFreq.valueOf(runFreq)) {
		case minute:
			isOver = TimeUtils.compareTwoDateBigOrSmall(preDateArgs.replaceAll("-+|\\s+|:+", "").substring(0, 10), dateArgs.replaceAll("-+|\\s+|:+", "").substring(0, 10), "yyyyMMddHH", "yyyyMMddHH");
			break;
		case hour:
			isOver = TimeUtils.compareTwoDateBigOrSmall(preDateArgs.replaceAll("-+|\\s+|:+", "").substring(0, 8), dateArgs.replaceAll("-+|\\s+|:+", "").substring(0, 8), "yyyyMMdd", "yyyyMMdd");
			break;
		case day:
			isOver = TimeUtils.compareTwoDateBigOrSmall(preDateArgs.replaceAll("-+|\\s+|:+", "").substring(0, 6), dateArgs.replaceAll("-+|\\s+|:+", "").substring(0, 6), "yyyyMM", "yyyyMM");
			break;
		case month:
			isOver = TimeUtils.compareTwoDateBigOrSmall(preDateArgs.replaceAll("-+|\\s+|:+", "").substring(0, 4), dateArgs.replaceAll("-+|\\s+|:+", "").substring(0, 4), "yyyy", "yyyy");
			break;		
		default:
			throw new Exception(runFreq + " RunFreq can't be process!");
		}
		return isOver;
	}
	
	
	/**
	 * 上一个批次任务是否在运行
	 * @return
	 * @throws Exception 
	 */
	private boolean isPreTaskRunSuc(String xmlid,String runFreq,String cronExp,boolean isTimeTask,boolean isCycle,String dateArgs) throws Exception {
		String[] procFileds = new String[] { "xmlid", "date_args","valid_flag","task_state" };
		String preDateArgs = null;
		//时间触发的任务
		if(isTimeTask){
			preDateArgs = this.getCronExpLastFireTime(cronExp, dateArgs, runFreq,Integer.parseInt(MemCache.PROC_MAP.get(xmlid).getDateArgs()));
			if(StringUtils.isEmpty(preDateArgs)){
				return true;
			}
		//事件任务
		}else{
			preDateArgs = this.getPreDateArgsForEventTask(runFreq,dateArgs);
		}
		
		//上一次周期是否已经越界周期,如果是则说明当前周期是周期内第一批次
		if(isCycle && overCycle(preDateArgs,dateArgs,runFreq)){
			return true;
		}
		String rmKey = xmlid + "@" + dateArgs;
		if(MemCache.RUNMODE_EVENT.containsKey(rmKey)){
			if(MemCache.RUNMODE_EVENT.get(rmKey).get(xmlid + "@" + preDateArgs) == 1){
				MemCache.RUNMODE_EVENT.remove(rmKey);
				return true;
			}else{
				return false;
			}
		}
		int result = search.checkExistLastNon("proc_schedule_log", procFileds, new Object[] {xmlid,preDateArgs,IsValid.VALID_FLAG,RunStatus.PROC_RUN_SUCCESS});
		if(result > 0){
			if(!MemCache.RUNMODE_EVENT.containsKey(rmKey)){
				MemCache.RUNMODE_EVENT.put(rmKey, new ConcurrentHashMap<String,Integer>());
				MemCache.RUNMODE_EVENT.get(rmKey).put(xmlid + "@" + preDateArgs, 0);
			}
			return false;
		}else{
			return true;
		}
	}
	
	public static String getPreBatch(String cronExp, String dateArgs,String runFreq) throws Exception {
		
		if(CronExpression.isValidExpression(cronExp)) {
			throw new Exception(cronExp + " not a valid cronExp!");
		} 
		CronExpression cronExpression = new CronExpression(cronExp);
		SimpleDateFormat dateFormater = null;
		switch(RunFreq.valueOf(runFreq)){
			case month : dateFormater=new SimpleDateFormat("yyyy-MM"); break;
			case day : dateFormater=new SimpleDateFormat("yyyy-MM-dd");break;
			case hour :  dateFormater=new SimpleDateFormat("yyyy-MM-dd HH");break;
			case minute :  dateFormater=new SimpleDateFormat("yyyy-MM-dd HH:mm");break;
			default: throw new Exception(runFreq + " RunFreq can't be process!");
		}
		Date cdate = dateFormater.parse(dateArgs);
		Calendar cal = Calendar.getInstance();
		
		while(true) {
			switch(RunFreq.valueOf(runFreq)){
			case month:
				cal.setTime(cdate);
				cal.add(Calendar.MONTH, -1);
				cdate=cal.getTime();
				if(cronExpression.isSatisfiedBy(cdate)) {
					return dateFormater.format(cdate);
				}
				break;
			case day:
				cal.setTime(cdate);
				cal.add(Calendar.DATE, -1);
				cdate=cal.getTime();
				if(cronExpression.isSatisfiedBy(cdate)) {
					return dateFormater.format(cdate);
				}
				break;
			case hour:
				cal.setTime(cdate);
				cal.add(Calendar.HOUR, -1);
				cdate=cal.getTime();
				if(cronExpression.isSatisfiedBy(cdate)) {
					return dateFormater.format(cdate);
				} 
				break;
			case minute:
				cal.setTime(cdate);
				cal.add(Calendar.MINUTE, -1);
				cdate=cal.getTime();
				if(cronExpression.isSatisfiedBy(cdate)) {
					return dateFormater.format(cdate);
				}  
				break;
			default:
				throw new Exception(runFreq + " RunFreq can't be process!");
			}
		}
	}
		
	public static void main(String[] args) {
		
		Date date = null;
		try {
			date = TimeUtils.string2Date("2019-07-30 01:07:01 ","yyyy-MM-dd HH:mm:ss");
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		
		CronExpression cron ;
		try {
			cron = new CronExpression("0 7/15 * * * ?");
			System.out.println(cron.isSatisfiedBy(date));
			
			Date dd = CheckRunModelHandler.getConExpNearPastFireTimeMinute("0 7/15 * * * ?", date);
			System.out.println(dd.toLocaleString());
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}