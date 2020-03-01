package com.asiainfo.dacp.dp.tools;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.asiainfo.dacp.dp.server.scheduler.type.DataFreq;
import com.asiainfo.dacp.dp.server.scheduler.type.RunFreq;

/**
 * 时间处理通用类
 * @author run[zhangqi@lianchuang.com]
 * 4:02:17 PM Sep 27, 2009
 */
public class TimeUtils {
	
	public static String Defaultformat = "yyyyMMddHHmmss";
		
	/**
	 * long转String
	 * yyyyMMddHHmmss
	 * @param longTime
	 * @return
	 */
	public static String getDateStringFromLong(long longTime) {
		return getDateStringFromLong(longTime, Defaultformat);
	}
	
	/**
	 * long转String
	 * @param longTime
	 * @param format
	 * @return
	 */
	public static String getDateStringFromLong(long longTime, String format) {
		java.util.Date date = new java.util.Date(longTime);
		return date2String(date, format);
	}
	
	/**
	 * 得到当前时间(Date To String)
	 * yyyyMMddHHmmss
	 * @return
	 */
	public static String getCurrentTime() {
		return getCurrentTime(Defaultformat);
	}
	
	/**
	 * 得到昨天的时间
	 * @return
	 */
	public static String getYesterdayDate(){
		return getYesterdayDate(Defaultformat);
	}
		
	/**
	 * 得到昨天的时间
	 * @return
	 */
	public static String getYesterdayDate(String format) {
		SimpleDateFormat sf = new SimpleDateFormat(format);
		Date today = new Date();
		today.setTime(today.getTime() - 60 * 24 * 60 * 1000);
		return sf.format(today).toString();
	}
	
	/**
	 * 得到当前时间(Date To String)
	 * @param format
	 * @return
	 */
	public static String getCurrentTime(String format) {
		return date2String(new java.util.Date(), format);
	}

	/**
	 * 使用指定的格式格式当前的日期对象
	 * @param obj
	 * @param format
	 * @return
	 */
	public static String date2String(java.util.Date obj, String format) {
		if (obj == null) {
			obj = new java.util.Date();
		}
		SimpleDateFormat dateFormater = new SimpleDateFormat(format);
		return dateFormater.format(obj);
	}
	
	/**
	 * 把时间串按照响应的格式转换成日期对象
	 * @param dateStr
	 * @param format
	 * @return
	 * @throws ParseException
	 */
    public static java.util.Date string2Date(String dateStr, String format) throws ParseException {
        if (StringUtils.isEmpty(dateStr) || StringUtils.isEmpty(format)){
        	throw new RuntimeException("日期转换失败:dateStr or format is null");
        }
        SimpleDateFormat dateFormater = new SimpleDateFormat(format);
        return dateFormater.parse(dateStr);
    }
    
    /**
     * Date To Timestamp
     * @param date
     * @return
     */
    public static Timestamp utilDate2Timestamp(java.util.Date date) {
        return new java.sql.Timestamp(date == null ? new java.util.Date().getTime() : date.getTime());
    }
    
    /**
     * String To Timestamp
     * @param dateStr
     * @param format
     * @return
     * @throws Exception
     */
    public static java.sql.Timestamp string2Timestamp(String dateStr, String format) throws Exception {
		if (StringUtils.isEmpty(dateStr)) {
			throw new RuntimeException("日期转换失败:dateStr is null");
		}
		return utilDate2Timestamp(string2Date(dateStr, format));
	}
    
    /**
     * Timestamp To String
     * @param timestamp
     * @param format
     * @return
     */
    public static String convertString2Timestamp(Timestamp timestamp, String format){
    	Date date = new Date(timestamp.getTime());
    	return date2String(date, format);
    }
    
    /**
     * 得到当前时间的java.sql.Date对象
     * @return
     */
    public static java.sql.Date getCurrentSQLDate() {
        return new java.sql.Date(new java.util.Date().getTime());
    }
    
    /**
     * 把原始格式的String转换为目标格式的String
     * @param dateStr
     * @param origFormat
     * @param targetFormat
     * @return
     * @throws Exception
     */
    public static String convertDateForm(String dateStr, String origFormat, String targetFormat) throws Exception{
        SimpleDateFormat origFormater = new SimpleDateFormat(origFormat);
        Date date = origFormater.parse(dateStr);
        SimpleDateFormat targetFormater = new SimpleDateFormat(targetFormat);
        return targetFormater.format(date);
    }
    
    /**
     * 比较2个String对应的时间的大小
     * @param fromDate
     * @param toDate
     * @param fromFormat
     * @param toFormat
     * @return
     * @throws ParseException
     */
	public static boolean compareTwoDateBigOrSmall(String fromDate, String toDate, String fromFormat, String toFormat) throws ParseException {
		return string2Date(fromDate, fromFormat).before(string2Date(toDate, toFormat));
	}
    
	/**
	 * 计算时间差(天)
	 * @param date1
	 * @param date2
	 * @return
	 */
	public static int getTimeDifferential(Date date1, Date date2){
		if(date1 == null || date2 == null){
			throw new RuntimeException("计算时间差,参数不能为NULL");
		}
		return (int)((date1.getTime() - date2.getTime()) / (24 * 60 * 60 * 1000));
	}
    
	/**
	 * 计算时间间隙,用于观察代码段耗时
	 * @param startTime
	 * @return
	 */
	public static String calcInterval(Date startTime){
		String tmp = String.valueOf(System.currentTimeMillis() - startTime.getTime());
		startTime.setTime(System.currentTimeMillis());
        return tmp;
	}
	
	
	/**
	 * 将毫秒数换算成x天x时x分x秒x毫秒
	 * @param ms
	 * @return
	 */
	public static String formatLong(long ms) {
		int ss = 1000;
		int mi = ss * 60;
		int hh = mi * 60;
		int dd = hh * 24;

		long day = ms / dd;
		long hour = (ms - day * dd) / hh;
		long minute = (ms - day * dd - hour * hh) / mi;
		long second = (ms - day * dd - hour * hh - minute * mi) / ss;
		long milliSecond = ms - day * dd - hour * hh - minute * mi - second * ss;

		String strDay = day < 10 ? "0" + day : "" + day;
		String strHour = hour < 10 ? "0" + hour : "" + hour;
		String strMinute = minute < 10 ? "0" + minute : "" + minute;
		String strSecond = second < 10 ? "0" + second : "" + second;
		String strMilliSecond = milliSecond < 10 ? "0" + milliSecond : ""
				+ milliSecond;
		strMilliSecond = milliSecond < 100 ? "0" + strMilliSecond : ""
				+ strMilliSecond;
		return strDay + " " + strHour + ":" + strMinute + ":" + strSecond + " "
				+ strMilliSecond;
	}

	/**
	 * 根据依赖配置,计算依赖周期
	 * PROC类型yyyy-MM-dd,其他yyyyMMdd
	 * ML月末限定为上月最后一天(PROC类型yyyy-MM-dd,其他yyyyMMdd)
	 * @param dependCfg
	 * @param sRunFreq
	 * @param sDateArgs
	 * @param isProc
	 * @return
	 * @throws Exception
	 */
	public static String getDependDateArgs(String dependCfg, String sDateArgs, boolean isProc) throws Exception {
		if (StringUtils.isEmpty(dependCfg)) {
			throw new Exception("dependCfg can't be Empty!");
		}
		String[] a = dependCfg.split("-");
		if (a.length != 2 && a.length != 3) {
			throw new Exception(dependCfg + "dependCfg of format is error");
		}		
		Calendar ca = Calendar.getInstance();		
		String freq = a[0];
		String offset = a.length==3 ? "-".concat(a[2]) : a[1];
		DataFreq _cycleType = DataFreq.valueOf(freq.toUpperCase());
		switch (_cycleType) {
		case D:
			ca.setTime(TimeUtils.string2Date(sDateArgs, "yyyy-MM-dd"));
			ca.add(Calendar.DATE, 0 - Integer.parseInt(offset));
			return isProc ? TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd"):TimeUtils.date2String(ca.getTime(),"yyyyMMdd");
		case M:
			ca.setTime(TimeUtils.string2Date(sDateArgs, "yyyy-MM-dd"));
			ca.add(Calendar.MONTH, 0 - Integer.parseInt(offset));
			return isProc ? TimeUtils.date2String(ca.getTime(),"yyyy-MM")+"-01":TimeUtils.date2String(ca.getTime(),"yyyyMM")+"01";
		case Y:
			ca.setTime(TimeUtils.string2Date(sDateArgs, "yyyy-MM-dd"));
			ca.add(Calendar.YEAR, 0 - Integer.parseInt(offset));
			return isProc ? TimeUtils.date2String(ca.getTime(),"yyyy")+"-01-01":TimeUtils.date2String(ca.getTime(),"yyyy")+"0101";
		case ML:
			ca.setTime(TimeUtils.string2Date(sDateArgs, "yyyy-MM-dd"));
			ca.add(Calendar.MONTH, 1 - Integer.parseInt(offset));
			ca.set(Calendar.DAY_OF_MONTH, 0);
			return isProc ? TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd"):TimeUtils.date2String(ca.getTime(),"yyyyMMdd");
		case DL:
			ca.setTime(TimeUtils.string2Date(sDateArgs, "yyyy-MM-dd"));
			ca.add(Calendar.DATE, 0 - Integer.parseInt(offset));
			return isProc ? TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd 23"):TimeUtils.date2String(ca.getTime(),"yyyyMMdd23");
		case H:
			ca.setTime(TimeUtils.string2Date(sDateArgs, "yyyy-MM-dd HH"));
			ca.add(Calendar.HOUR, 0 - Integer.parseInt(offset));
			return isProc ? TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd HH"):TimeUtils.date2String(ca.getTime(),"yyyyMMddHH");
		case MI:
			ca.setTime(TimeUtils.string2Date(sDateArgs, "yyyy-MM-dd HH:mm"));
			ca.add(Calendar.MINUTE, 0 - Integer.parseInt(offset));
			return isProc ? TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd HH:mm"):TimeUtils.date2String(ca.getTime(),"yyyyMMddHHmm");
		case DH:
			ca.setTime(TimeUtils.string2Date(sDateArgs, "yyyy-MM-dd"));
			ca.add(Calendar.DATE, 0 - Integer.parseInt(offset.split("#")[0]));
			return isProc ? TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd "+offset.split("#")[1]):TimeUtils.date2String(ca.getTime(),"yyyyMMdd"+offset.split("#")[1]);
		case DD:
			ca.setTime(TimeUtils.string2Date(sDateArgs, "yyyy-MM-dd"));
			ca.add(Calendar.MONTH, 0 - Integer.parseInt(offset.split("#")[0]));
			return isProc ? TimeUtils.date2String(ca.getTime(),"yyyy-MM-"+offset.split("#")[1]):TimeUtils.date2String(ca.getTime(),"yyyyMM"+offset.split("#")[1]);
		default:
			throw new Exception(_cycleType + "dependCfg of format is error");
		}
	}
	
	/**
	 * 获取程序触发数据时间
	 * @param targetCfg
	 * @param sourceDataArgs
	 * @param sourceRunfreq
	 * @return
	 * @throws Exception
	 */
	public static String getProcTargetDataDataTime(String targetCfg, String sourceDataArgs) throws Exception {
		if (StringUtils.isEmpty(targetCfg)) {
			throw new Exception("targetCfg can't be Empty!");
		}
		String[] a = targetCfg.split("-");
		if (a.length != 2 && a.length != 3) {
			throw new Exception(targetCfg + "targetCfg of format is error");
		}
		
		if(StringUtils.isEmpty(sourceDataArgs)){
			return null;
		}
		Calendar ca = Calendar.getInstance();
		String freq = a[0];
		int offset = a.length==3 ? -Integer.parseInt(a[2]) : Integer.parseInt(a[1]);
		DataFreq _cycleType = DataFreq.valueOf(freq);
		switch (_cycleType) {
		case D:
			ca.setTime(TimeUtils.string2Date(sourceDataArgs, "yyyy-MM-dd"));
			ca.add(Calendar.DATE, 0 - offset);
			return TimeUtils.date2String(ca.getTime(),"yyyyMMdd");
		case M:
			ca.setTime(TimeUtils.string2Date(sourceDataArgs, "yyyy-MM-dd"));
			ca.add(Calendar.MONTH, 0 - offset);
			return TimeUtils.date2String(ca.getTime(),"yyyyMM")+"01";
		case Y:
			ca.setTime(TimeUtils.string2Date(sourceDataArgs, "yyyy-MM-dd"));
			ca.add(Calendar.YEAR, 0 - offset);
			return TimeUtils.date2String(ca.getTime(),"yyyy")+"0101";
		case ML:
			ca.setTime(TimeUtils.string2Date(sourceDataArgs, "yyyy-MM-dd"));
			ca.add(Calendar.MONTH, 1 - offset);
			ca.set(Calendar.DAY_OF_MONTH, 0);
			return TimeUtils.date2String(ca.getTime(),"yyyyMMdd");
		case DL:
			ca.setTime(TimeUtils.string2Date(sourceDataArgs, "yyyy-MM-dd"));
			ca.add(Calendar.DATE, 0 - offset);
			return TimeUtils.date2String(ca.getTime(),"yyyyMMdd23");
		case H:
			if(sourceDataArgs.length()==13) {
				ca.setTime(TimeUtils.string2Date(sourceDataArgs, "yyyy-MM-dd HH"));
				ca.add(Calendar.HOUR, 0 - offset);
				return TimeUtils.date2String(ca.getTime(),"yyyyMMddHH");
			}
		case MI:
			if(sourceDataArgs.length()==16) {
				ca.setTime(TimeUtils.string2Date(sourceDataArgs, "yyyy-MM-dd HH:mm"));
				ca.add(Calendar.MINUTE, 0 - offset);
				return TimeUtils.date2String(ca.getTime(),"yyyyMMddHHmm");
			}
		default:
			throw new Exception(_cycleType + " targetCfg of format is error");
		}
	}
	
	
	/**
	 * 获取程序触发程序的dateArgs
	 * @param dependCfg
	 * @param sourceDataArgs
	 * @return
	 * @throws Exception
	 */
	public static String getProcTargetProcDateArgs(String dependCfg, String sourceDataArgs) throws Exception {
		if (StringUtils.isEmpty(dependCfg)) {
			throw new Exception("dependCfg can't be Empty!");
		}
		String[] a = dependCfg.split("-");
		if (a.length != 2 && a.length != 3) {
			throw new Exception(dependCfg + "dependCfg of format is error");
		}
		
		if(StringUtils.isEmpty(sourceDataArgs)){
			return null;
		}
		Calendar ca = Calendar.getInstance();		
		String freq = a[0];
		String offset = a.length==3 ? "-".concat(a[2]) : a[1];
		DataFreq _cycleType = DataFreq.valueOf(freq);
		switch (_cycleType) {
		case D:
			ca.setTime(TimeUtils.string2Date(sourceDataArgs, "yyyy-MM-dd"));
			ca.add(Calendar.DATE, Integer.parseInt(offset));
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd");
		case M:
			ca.setTime(TimeUtils.string2Date(sourceDataArgs, "yyyy-MM-dd"));
			ca.add(Calendar.MONTH, Integer.parseInt(offset));
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM")+"-01";
		case Y:
			ca.setTime(TimeUtils.string2Date(sourceDataArgs, "yyyy-MM-dd"));
			ca.add(Calendar.YEAR, Integer.parseInt(offset));
			return TimeUtils.date2String(ca.getTime(),"yyyy")+"-01-01";
		case ML:
			ca.setTime(TimeUtils.string2Date(sourceDataArgs, "yyyy-MM-dd"));
			//ca.add(Calendar.MONTH, 1 - offset);
			ca.add(Calendar.MONTH, Integer.parseInt(offset));
			ca.set(Calendar.DAY_OF_MONTH,1);
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd");
		case DL:
			ca.setTime(TimeUtils.string2Date(sourceDataArgs, "yyyy-MM-dd HH"));
			//ca.add(Calendar.DAY_OF_MONTH, 1 - offset);
			ca.add(Calendar.DAY_OF_MONTH, Integer.parseInt(offset));
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd");
		case H:
			ca.setTime(TimeUtils.string2Date(sourceDataArgs, "yyyy-MM-dd HH"));
			ca.add(Calendar.HOUR, Integer.parseInt(offset));
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd HH");
		case MI:
			ca.setTime(TimeUtils.string2Date(sourceDataArgs, "yyyy-MM-dd HH:mm"));
			ca.add(Calendar.MINUTE, Integer.parseInt(offset));
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd HH:mm");
		case DH:
			ca.setTime(TimeUtils.string2Date(sourceDataArgs, "yyyy-MM-dd HH"));
			ca.add(Calendar.DATE, Integer.parseInt(offset.split("#")[0]));
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd");
		case DD:
			ca.setTime(TimeUtils.string2Date(sourceDataArgs, "yyyy-MM-dd"));
			ca.add(Calendar.MONTH, Integer.parseInt(offset.split("#")[0]));
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM")+"-01";	
		default:
			throw new Exception(_cycleType + "dependCfg of format is error");
		}
	}
	
	/**
	 * 获取数据(程序输出)触发程序的dateArgs
	 * @param dependCfg
	 * @param dataTime
	 * @param dataParentRunFreq
	 * @return
	 * @throws Exception
	 */
	public static String getDataTargetProcDateArgs(String dependCfg, String dataTime) throws Exception {
		if (StringUtils.isEmpty(dependCfg)) {
			throw new Exception("dependCfg can't be Empty!");
		}
		String[] a = dependCfg.split("-");
		if (a.length != 2 && a.length != 3) {
			throw new Exception(dependCfg + " dependCfg of format is error");
		}
		
		if(StringUtils.isEmpty(dataTime)){
			return null;
		}
		Calendar ca = Calendar.getInstance();
		String freq = a[0];
		int offset = a.length==3 ? -Integer.parseInt(a[2]) : Integer.parseInt(a[1]);
		DataFreq _cycleType = DataFreq.valueOf(freq);
		switch (_cycleType) {
		case D:
			ca.setTime(TimeUtils.string2Date(dataTime, "yyyyMMdd"));
			ca.add(Calendar.DATE, offset);
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd");
		case M:
			ca.setTime(TimeUtils.string2Date(dataTime, "yyyyMMdd"));
			ca.add(Calendar.MONTH, offset);
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM")+"-01";
		case Y:
			ca.setTime(TimeUtils.string2Date(dataTime, "yyyyMMdd"));
			ca.add(Calendar.YEAR, offset);
			return TimeUtils.date2String(ca.getTime(),"yyyy")+"-01-01";
		case ML:
			ca.setTime(TimeUtils.string2Date(dataTime, "yyyyMMdd"));
			//ca.add(Calendar.MONTH, 1 - offset);
			ca.add(Calendar.MONTH, offset);
			ca.set(Calendar.DAY_OF_MONTH,1);
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd");
		case DL:
			ca.setTime(TimeUtils.string2Date(dataTime, "yyyyMMddHH"));
			//ca.add(Calendar.DAY_OF_MONTH, 1 - offset);
			ca.add(Calendar.DAY_OF_MONTH, offset);
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd");
		case H:
			ca.setTime(TimeUtils.string2Date(dataTime, "yyyyMMddHH"));
			ca.add(Calendar.HOUR, offset);
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd HH");
		case MI:
			ca.setTime(TimeUtils.string2Date(dataTime, "yyyyMMddHHmm"));
			ca.add(Calendar.MINUTE, offset);
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd HH:mm");
		default:
			throw new Exception(_cycleType + " dependCfg of format is error");
		}
	}
	
	
	
	/**
	 * 获取数据(外部接口)触发程序的dateArgs
	 * @param dependCfg
	 * @param dataTime
	 * @param dataParentRunFreq
	 * @return
	 * @throws Exception
	 */
	public static String getIntfDataTargetProcDateArgs(String dependCfg, String dataTime) throws Exception {
		if (StringUtils.isEmpty(dependCfg)) {
			throw new Exception("dependCfg can't be Empty!");
		}
		String[] a = dependCfg.split("-");
		if (a.length != 2 && a.length != 3) {
			throw new Exception(dependCfg + "dependCfg of format is error");
		}
		
		if(StringUtils.isEmpty(dataTime)){
			return null;
		}
		Calendar ca = Calendar.getInstance();
		TimeUtils.getCalendarFromIntfDataTime(dataTime,ca);
		String freq = a[0];
		int offset = a.length==3 ? -Integer.parseInt(a[2]) : Integer.parseInt(a[1]);
		DataFreq _cycleType = DataFreq.valueOf(freq);
		switch (_cycleType) {
		case D:
			ca.add(Calendar.DATE, offset);
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd");
		case M:
			ca.add(Calendar.MONTH, offset);
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM")+"-01";
		case Y:
			ca.add(Calendar.YEAR, offset);
			return TimeUtils.date2String(ca.getTime(),"yyyy")+"-01-01";
		case ML:
			//ca.add(Calendar.MONTH, 1 - offset);
			ca.add(Calendar.MONTH, offset);
			ca.set(Calendar.DAY_OF_MONTH,1);
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd");
		case DL:
			//ca.add(Calendar.DAY_OF_MONTH, 1 - offset);
			ca.add(Calendar.DAY_OF_MONTH, offset);
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd");
		case H:
			ca.add(Calendar.HOUR, offset);
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd HH");
		case MI:
			ca.add(Calendar.MINUTE, offset);
			return TimeUtils.date2String(ca.getTime(),"yyyy-MM-dd HH:mm");
		default:
			throw new Exception(_cycleType + " dependCfg of format is error");
		}
	}
	
	
	/**
	 * 外部数据时间转换为Calendar
	 * @param dataTime
	 * @return
	 * @throws Exception
	 */
	private static void getCalendarFromIntfDataTime(String dataTime,Calendar ca) throws Exception {
		if(StringUtils.isEmpty(dataTime)){
			return;
		}
		switch (dataTime.length()) {
		case 6:
			ca.setTime(TimeUtils.string2Date(dataTime, "yyyyMM"));
			break;
		case 8:
			ca.setTime(TimeUtils.string2Date(dataTime, "yyyyMMdd"));
			break;
		case 10:
			ca.setTime(TimeUtils.string2Date(dataTime, "yyyyMMddHH"));
			break;
		case 12:
			ca.setTime(TimeUtils.string2Date(dataTime, "yyyyMMddHHmm"));
			break;
		default:
			throw new Exception(dataTime + " intf dataTime can't be process!");
		}
	}
	
	/**
	 * 转换DataTime(重庆专用)
	 * @param dataTime
	 * @return
	 * @throws Exception
	 */
	public static String convertDataTime2CQ(String dataTime) throws Exception {
		if(StringUtils.isEmpty(dataTime)){
			return null;
		}
		switch (dataTime.length()) {
		case 6:
			return TimeUtils.date2String(TimeUtils.string2Date(dataTime, "yyyyMM"), "yyyy-MM" + "-01");
		case 8:
			return TimeUtils.date2String(TimeUtils.string2Date(dataTime, "yyyyMMdd"), "yyyy-MM-dd");
		case 10:
			return TimeUtils.date2String(TimeUtils.string2Date(dataTime, "yyyyMMddHH"), "yyyy-MM-dd HH");
		case 12:
			return TimeUtils.date2String(TimeUtils.string2Date(dataTime, "yyyyMMddHHmm"), "yyyy-MM-dd HH:mm");
		case 14:
			return TimeUtils.date2String(TimeUtils.string2Date(dataTime, "yyyyMMddHHmmss"), "yyyy-MM-dd HH:mm:ss");
		default:
			throw new Exception(dataTime + " CQ dataTime can't be process!");
		}
	}
	
	
	
	/**
	 * 获取dataArgs(仅限时间触发使用)
	 * @param runFreq
	 * @param date
	 * @param offset
	 * @return
	 * @throws Exception
	 */
	public static String getDateArgs(String runFreq, Date date,int offset) throws Exception {
		Calendar ca = Calendar.getInstance();
		ca.setTime(date);
		RunFreq _cycleType = RunFreq.valueOf(runFreq);
		switch (_cycleType) {
		case year:
			ca.add(Calendar.YEAR, 0 - offset);
			return TimeUtils.date2String(ca.getTime(), "yyyy")+"-01-01";
		case day:
			ca.add(Calendar.DATE, 0 - offset);
			return TimeUtils.date2String(ca.getTime(), "yyyy-MM-dd");
		case month:
			ca.add(Calendar.MONTH, 0 - offset);
			return TimeUtils.date2String(ca.getTime(), "yyyy-MM")+"-01";
		case hour:
			ca.add(Calendar.HOUR, 0 - offset);
			return TimeUtils.date2String(ca.getTime(), "yyyy-MM-dd HH");
		case minute:
			ca.add(Calendar.MINUTE, 0 - offset);
			return TimeUtils.date2String(ca.getTime(), "yyyy-MM-dd HH:mm");
		default:
			throw new Exception(runFreq + " RunFreq can't be process!");
		}
	}	
	
	/**
	 * 是否为数字
	 * @param str
	 * @return
	 */
	public static boolean isNumeric(String str) {
		Pattern pattern = Pattern.compile("[0-9]*");
		Matcher isNum = pattern.matcher(str);
		if (!isNum.matches()) {
			return false;
		}
		return true;
	}
	
	/**
	 * 获取月初DATE
	 * @return
	 */
	public static Date getInEarlyOfMon(){
		Calendar ca = Calendar.getInstance();
		ca.add(Calendar.MONTH, 0);
		ca.set(Calendar.DAY_OF_MONTH,1);
		return ca.getTime();
	}
	
	/**
	 * 是否大于今天的账期
	 * @param dateArgs
	 * @return
	 * @throws Exception 
	 */
	public static boolean isBefore2Tomorrow(String dateArgs) throws Exception{
		if(StringUtils.isEmpty(dateArgs)){
			return false;
		}		
		dateArgs = dateArgs.replaceAll("-+|\\s+|:+", "");
		Calendar ca = Calendar.getInstance();
		ca.setTime(TimeUtils.string2Date(TimeUtils.getCurrentTime("yyyyMMdd"), "yyyyMMdd"));
		ca.add(Calendar.DAY_OF_MONTH,1);
		switch (dateArgs.length()) {
		case 8:
			return TimeUtils.string2Date(dateArgs, "yyyyMMdd").before(ca.getTime());
		case 10:
			return TimeUtils.string2Date(dateArgs, "yyyyMMddHH").before(ca.getTime());
		case 12:
			return TimeUtils.string2Date(dateArgs, "yyyyMMddHHmm").before(ca.getTime());
		default:
			throw new Exception(dateArgs + " can't be process!");
		}
	}
	
	/**
	 * 是否日末,月末
	 * @param runFreq
	 * @param dataArgs
	 * @return
	 * @throws Exception 
	 */
	public static boolean isLast(String runFreq, String dataArgs,boolean isData) throws Exception{
		Calendar ca = Calendar.getInstance();
		RunFreq _cycleType = RunFreq.valueOf(runFreq);
		String format = null;
		switch (_cycleType) {
		case day:
			format = isData ? "yyyyMMdd":"yyyy-MM-dd";
			ca.setTime(TimeUtils.string2Date(dataArgs, format));
			ca.add(Calendar.DAY_OF_MONTH, 1);
			return ca.get(Calendar.DAY_OF_MONTH) == 1;
		case hour:
			format = isData ? "yyyyMMddHH":"yyyy-MM-dd HH";
			return "23".equals(TimeUtils.date2String(TimeUtils.string2Date(dataArgs, format), "HH"));
		case minute:
			format = isData ? "yyyyMMddHHmm":"yyyy-MM-dd HH:mm";
			return "59".equals(TimeUtils.date2String(TimeUtils.string2Date(dataArgs, format), "mm"));
		default:
			return false;
		}
	}
	
	/**
	 * 是否指定时间
	 * @param runFreq
	 * @param dataArgs
	 * @param isData
	 * @return
	 * @throws Exception
	 */
	public static boolean isDesignated(String runFreq, String desTime,String dataArgs,boolean isData) throws Exception{
		RunFreq _cycleType = RunFreq.valueOf(runFreq);
		String format = null;
		switch (_cycleType) {
		case day:
			format = isData ? "yyyyMMdd":"yyyy-MM-dd";
			return TimeUtils.date2String(TimeUtils.string2Date(dataArgs, format), "dd").equals(desTime);
		case hour:
			format = isData ? "yyyyMMddHH":"yyyy-MM-dd HH";
			return TimeUtils.date2String(TimeUtils.string2Date(dataArgs, format), "HH").equals(desTime);
		default:
			return false;
		}
	}
	
	/**
	 * 格式化时间格式
	 * @param runFreq
	 * @param dateArgs
	 * @return
	 * @throws Exception
	 */
	public static Date formatStr2Date(String runFreq, String dateArgs) throws Exception{
		RunFreq _cycleType = RunFreq.valueOf(runFreq);
		SimpleDateFormat format = null;
		switch (_cycleType) {
		case day:
			format = new SimpleDateFormat("yyyy-MM-dd");
			return format.parse(dateArgs);
		case hour:
			format = new SimpleDateFormat("yyyy-MM-dd HH");
			return format.parse(dateArgs);
		case minute:
			format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			return format.parse(dateArgs);
		case month:
			format = new SimpleDateFormat("yyyy-MM-dd");
			return format.parse(dateArgs);		
		default:
			return null;
		}
	}
	
	/**
	 * 获取时间差数组
	 * @param runFreq
	 * @param startDataArgs
	 * @param endDataArgs
	 * @return
	 */
	public List<String> getTimeDiff(String runFreq, String startDataArgs,String endDataArgs){
		return null;
	}
	
	
	/**
	 * 逆向分析账期
	 * @param runFreq
	 * @param dateArgs
	 * @return
	 * @throws Exception
	 */
	public static List<String> getCycleReverse(String targetDateArgs) throws Exception{
		if(StringUtils.isEmpty(targetDateArgs)){
			return null;
		}
		List<String> l = new ArrayList<String>();
		boolean isData = targetDateArgs.contains("-");
		targetDateArgs = targetDateArgs.replaceAll("-+|\\s+|:+", "");
		Calendar maxCycle = Calendar.getInstance();
		Calendar source = Calendar.getInstance();
		
		switch (targetDateArgs.length()) {
		case 8:			
			maxCycle.setTime(TimeUtils.string2Date(targetDateArgs, "yyyyMMdd"));
			maxCycle.set(Calendar.DAY_OF_MONTH, 1);
			maxCycle.add(Calendar.MONTH, 1);	        
			source.setTime(TimeUtils.string2Date(targetDateArgs, "yyyyMMdd"));
			
			while(maxCycle.after(source)){
				l.add(TimeUtils.date2String(source.getTime(), isData ? "yyyy-MM-dd" : "yyyyMMdd"));
				source.add(Calendar.DAY_OF_MONTH,1);	
			}
			break;
		case 10:
			maxCycle.setTime(TimeUtils.string2Date(targetDateArgs, "yyyyMMddHH"));
			maxCycle.set(Calendar.HOUR_OF_DAY, 0);
			maxCycle.add(Calendar.DAY_OF_MONTH, 1);	        
			source.setTime(TimeUtils.string2Date(targetDateArgs, "yyyyMMddHH"));
			
			while(maxCycle.after(source)){
				l.add(TimeUtils.date2String(source.getTime(), isData ? "yyyy-MM-dd HH" : "yyyyMMddHH"));
				source.add(Calendar.HOUR_OF_DAY,1);	
			}
			break; 
		case 12:
			
		default:
			
		}
		return l;
	}
}
