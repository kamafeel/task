//package com.asiainfo.dacp.dp.tools;
//
//import org.hyperic.sigar.FileSystem;
//import org.hyperic.sigar.FileSystemUsage;
//import org.hyperic.sigar.NetInterfaceConfig;
//import org.hyperic.sigar.NetInterfaceStat;
//import org.hyperic.sigar.Sigar;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.File;
//import java.lang.management.OperatingSystemMXBean;
//import java.lang.reflect.Field;
//import java.lang.reflect.Method;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//
//public class VMInfoUtil {
//    private static final Logger LOG = LoggerFactory.getLogger(VMInfoUtil.class);
//    static final long KB = 1024;
//    static final long MB = 1024 * 1024;
//    static final long GB = 1024 * 1024 * 1024;
//    public static Object lock = new Object();
//    private static VMInfoUtil vmInfo;
//    
//    private Sigar sigar;
//    private final OperatingSystemMXBean osMXBean;
//    
//    public static VMInfoUtil getVmInfo() {
//        if (vmInfo == null) {
//            synchronized (lock) {
//                if (vmInfo == null) {
//                    try {
//                        vmInfo = new VMInfoUtil();
//                    } catch (Exception e) {
//                        LOG.warn("no need care, the fail is ignored : vmInfo init failed " + e.getMessage(), e);
//                    }
//                }
//            }
//        }
//        return vmInfo;
//    }
//
//    
//    private VMInfoUtil() {
//        //初始化静态信息
//        osMXBean = java.lang.management.ManagementFactory.getOperatingSystemMXBean();
//        
//        try {
//        	String libdir=System.getProperty("etl.real.workspace")+File.separator+"monitorlibs";
//			this.addLibraryDir(libdir);
//			LOG.info("libdir:{}",libdir);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//        sigar = new Sigar();
//    }
//
//    public static long getLongFromOperatingSystem(OperatingSystemMXBean operatingSystem, String methodName) {
//        try {
//            final Method method = operatingSystem.getClass().getMethod(methodName, (Class<?>[]) null);
//            method.setAccessible(true);
//            return (Long) method.invoke(operatingSystem, (Object[]) null);
//        } catch (final Exception e) {
//            LOG.info(String.format("OperatingSystemMXBean %s failed, Exception = %s ", methodName, e.getMessage()));
//        }
//
//        return -1;
//    }
//    
//    public Map<String, Long> getPhyOSStatus(){
//    	Map<String, Long> PhyOSStatus = new HashMap<String, Long>();
//    	//物理内存
//    	PhyOSStatus.put("totalPhysicalMemory",VMInfoUtil.getLongFromOperatingSystem(osMXBean, "getTotalPhysicalMemorySize")/ MB);
//    	//可用内存
//    	PhyOSStatus.put("freePhysicalMemory",VMInfoUtil.getLongFromOperatingSystem(osMXBean, "getFreePhysicalMemorySize")/ MB);
//    	//最大文件打开数
//    	PhyOSStatus.put("maxFileDescriptorCount",VMInfoUtil.getLongFromOperatingSystem(osMXBean, "getMaxFileDescriptorCount"));
//    	//当前文件打开数
//    	PhyOSStatus.put("currentOpenFileDescriptorCount",VMInfoUtil.getLongFromOperatingSystem(osMXBean, "getOpenFileDescriptorCount"));
//    	try {
//    		// cpu使用百分比
//    		PhyOSStatus.put("cpuPerc",Double.valueOf(sigar.getCpuPerc().getCombined()*100).longValue());
//    		Long[] netall = netAll();
//        	PhyOSStatus.put("netIn",netall[0]);
//        	PhyOSStatus.put("netOut",netall[1]);
//    	} catch (Exception e) {
//    		e.printStackTrace();
//    	}
//    	
//        return PhyOSStatus;
//    } 
//    
//    public void update(){
//        sigar = new Sigar();
//    }
//    
//    
//    public Long[] netAll() throws Exception{
//    	Long[] result = {0l,0l};
//    	List<NetInterfaceStat> nets = netBytesAll();
//    	if(nets.size()==0) return result;
//    	update();
//        long time  = System.currentTimeMillis();
//        nets = netBytesAll();
//        long rx = 0;
//        long tx = 0;
//        for(int i=0;i<nets.size();i++) {
//        	rx+= nets.get(i).getRxBytes();
//        	tx+= nets.get(i).getTxBytes();
//        }
//        Thread.sleep(1000);
//        update();
//        time  = System.currentTimeMillis()-time;
//        nets = netBytesAll();
//        long rxa = 0;
//        long txa = 0;
//        for(int i=0;i<nets.size();i++) {
//        	rxa+= nets.get(i).getRxBytes();
//        	txa+= nets.get(i).getTxBytes();
//        }
//        result[0] = (rxa-rx)*1l/time;
//        result[1] = (txa-tx)*1l/time;
//    	return result;
//    }
//   
//    public List<NetInterfaceStat> netBytesAll() throws Exception {
//    	String ifNames[] = sigar.getNetInterfaceList();
//    	List<NetInterfaceStat> result = new ArrayList<NetInterfaceStat>();
//        for (int i = 0; i < ifNames.length; i++) {
//            String name = ifNames[i];
//            result.add(sigar.getNetInterfaceStat(name));
//        }
//        return result;
//    }
//    public Long[] net(String ip) throws Exception{
//    	Long[] result = {0l,0l};
//        if(netBytes(ip) == null) return result;
//        update();
//        long time  = System.currentTimeMillis();
//        long rx = netBytes(ip).getRxBytes();
//        long tx = netBytes(ip).getTxBytes();
//        Thread.sleep(500);
//        update();
//        time  = System.currentTimeMillis()-time;
//        rx = netBytes(ip).getRxBytes()-rx;
//        tx = netBytes(ip).getTxBytes()-tx;
//        result[0] = rx*1l/time;
//        result[1] = tx*1l/time;
//        return result;
//    }
//    
//    public NetInterfaceStat netBytes(String ip) throws Exception {
//        String ifNames[] = sigar.getNetInterfaceList();
//        NetInterfaceStat result = null;
//        for (int i = 0; i < ifNames.length; i++) {
//            String name = ifNames[i];
//            NetInterfaceConfig ifconfig = sigar.getNetInterfaceConfig(name);
//            if(ifconfig.getAddress().equals(ip)){
//                result = sigar.getNetInterfaceStat(name);
//                break;
//            }
//        }
//        return result;
//    }
//    
//    public Long[] getDiskIoInfo(String diskname) throws Exception {
//    	
//    	Long[] result = {0l,0l};
//        FileSystem fslist[] = sigar.getFileSystemList();
//        for (int i = 0; i < fslist.length; i++) {
//            FileSystem fs = fslist[i];
//            if(diskname.equalsIgnoreCase(fs.getDirName())){
//            	FileSystemUsage usage =sigar.getFileSystemUsage(fs.getDirName());
//            	result[0]=usage.getDiskReads();
//            	result[1]=usage.getDiskWrites();
//            }
//        }
//        return result;
//    }
//    
//    private static void addLibraryDir(String libraryPath) throws Exception {
//        Field userPathsField = ClassLoader.class.getDeclaredField("usr_paths");
//        userPathsField.setAccessible(true);
//        String[] paths = (String[]) userPathsField.get(null);
//        StringBuilder sb = new StringBuilder();
//        for (int i = 0; i < paths.length; i++) {
//            if (libraryPath.equals(paths[i])) {
//                return;
//            }
//            sb.append(paths[i]).append(File.pathSeparatorChar);
//        }
//        sb.append(libraryPath);
//        System.setProperty("java.library.path", sb.toString());
//        LOG.info("java.library.path:{}", sb.toString());
//        final Field sysPathsField = ClassLoader.class.getDeclaredField("sys_paths");
//        sysPathsField.setAccessible(true);
//        sysPathsField.set(null, null);
//    }
//
//    public static void main(String[] args) {
//    	VMInfoUtil vm = VMInfoUtil.getVmInfo();
//    	try {
//			System.out.println(vm.netAll());
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//    }
//}
