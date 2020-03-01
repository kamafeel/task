package com.asiainfo.dacp.dp.tools;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;


/**
 * 动态加载classpath
 * @author run[zhangqi@lianchuang.com]
 * 3:12:27 PM May 25, 2009
 */
public class ReflectSysClassLoader {
		
	public static URLClassLoader sysloader = (URLClassLoader)ClassLoader.getSystemClassLoader();
	private static Class<URLClassLoader> sysclass = URLClassLoader.class;
	private static Method method;
	
	/**
	 * jars以文件数组的形式传递
	 * @param pluginJar
	 * @throws Exception 
	 */
	public ReflectSysClassLoader(File[] pluginJar) throws Exception{
		for(File f : pluginJar){
			try {
				this.addURL(f.toURI().toURL());
			} catch (MalformedURLException e) {
				throw e;
			}
		}
	}
	
	public ReflectSysClassLoader(String jarDir) throws Exception{
		File[] fs = ReflectSysClassLoader.getFiles(jarDir);
		if(fs == null){
			return;
		}
		for(File f : fs){
			try {
				this.addURL(f.toURI().toURL());
			} catch (MalformedURLException e) {
				throw e;
			}
		}
	}
		
	public ReflectSysClassLoader(String[] pluginJar) throws Exception{
		for(String f : pluginJar){
			try {
				this.addURL(new File(f).toURI().toURL());
			} catch (MalformedURLException e) {
				throw e;
			}
		}
	} 
	
	/**
	 * 反射调用系统ClassLoad加载classpath
	 * @param url
	 * @throws Exception 
	 */	
	private void addURL(URL url) throws Exception {
        try {
	        if(method == null) {
	        	//封装系统URLClassLoader.addURL()方法的参数
	        	Class<?>[] cls = new Class[] {URL.class};
	        	//获得此方法的实例
	        	method = sysclass.getDeclaredMethod("addURL", cls);
	        	method.setAccessible(true);
	        }
	        //把自己的jar动态加载到系统的URLClassLoader里面
            method.invoke(sysloader, new Object[]{ url });
        } catch (Exception e) {
        	throw e;
        }
    }
	
	/**
	 * 获取文件夹下面所有文件(递归查找)
	 * @param srcDir
	 * @return
	 */
	public static File[] getFiles(String srcDir){
		File f = new File(srcDir);
		if(f.isDirectory()){
			return getFiles(f);
		}else{
			return null;
		}
	}
	
	/**
	 * 获取文件夹下面所有文件(递归查找)
	 * @param srcDir
	 * @return
	 */
	public static File[] getFiles(File srcDir){
		ArrayList<File> fl = getFiles(srcDir, null);
		File[] files = new File[fl.size()];
		int num = 0;
		for(File f : fl){
			files[num] = f;
			num++;
		}
		return files;
	}
	
	/**
	 * 获取文件夹下面所有文件(递归查找)
	 * @param srcDir
	 * @param f
	 * @return
	 */
	public static ArrayList<File> getFiles(File srcDir, ArrayList<File> f){
		ArrayList<File> fl = null;
		if (f == null) {
			fl = new ArrayList<File>();
		} else {
			fl = new ArrayList<File>(f);
		}
		/* 得到目录下的文件和目录数组 */
		File[] fileList = srcDir.listFiles();		
		for (File srcf : fileList) {
			if (srcf.isFile()) {
				fl.add(srcf);
			} else {
				// 递归调用,并赋值给fl
				fl = getFiles(srcf, fl);
			}
		}
        return fl;
	}
	
}
