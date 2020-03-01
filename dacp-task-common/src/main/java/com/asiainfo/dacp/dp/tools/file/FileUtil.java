package com.asiainfo.dacp.dp.tools.file;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * 文件处理类
 * @author run[zhangqi@lianchuang.com]
 * 3:39:17 PM May 26, 2009
 */

public class FileUtil {
	
	private static Logger logger = LoggerFactory.getLogger(FileUtil.class);
	
	private static final String Encode_GBK = "GBK";
	
	private static final int DEFAULT_BUFFER_SIZE = 1024 * 8;
	
	public static final String LINE_SEPARATOR;
	    static {
	    	//避免安全问题
	        StringWriter buf = new StringWriter(4);
	        PrintWriter out = new PrintWriter(buf);
	        //通过写入行分隔符字符串终止当前行
	        out.println();
	        LINE_SEPARATOR = buf.toString();
	    }
	
	
	
	/**
	 * 获取虚拟机当前可用内存*60%
	 * @return
	 */
	private static long freeMemory(){
		long size=(long)(Runtime.getRuntime().freeMemory()*(1.8f/3f)); 
		if(size>0){ 
			return size; 
		} else{
			return 1l; 
		}
    }

	/**
	 * 拷贝文件
	 * @param sourceFile
	 * @param destFile
	 */
	public static void copyFile(String sourceFile, String destFile){
		FileUtil.copyFile(new File(sourceFile),new File(destFile));
	}
	
	/**
	 * 批量拷贝文件到同一目标目录
	 * @param sourceFiles
	 * @param destDir
	 */
	public static void copyFilesToDirectory(File[] sourceFiles, String destDir){
		
		for(File sourceFile : sourceFiles){
			File destFile = new File(destDir + File.separator + sourceFile.getName());
			if(destFile.isFile()){
				destFile.delete();
			}
			copyFile(sourceFile, destFile);
		}
	}
	
	/**
	 * 拷贝文件(目标文件不存在,先建立)
	 * @param sourceFile
	 * @param destFile
	 */
	public static void copyFile(File sourceFile, File destFile) {
				
		//目标文件不存在,先建立
		if(!destFile.isFile()){
			try {
				if(destFile.getParentFile() != null){
					if(!destFile.getParentFile().isDirectory()){
						destFile.getParentFile().mkdirs();
					}
				}
				destFile.createNewFile();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		try {
			CopyFileNIOStream(sourceFile,destFile);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * 文件拷贝,NIO流
	 * @param srcFile
	 * @param destFile
	 * @throws IOException
	 */
	public static void CopyFileNIOStream(File srcFile, File destFile) throws IOException{
		FileInputStream input = new FileInputStream(srcFile);
		try {
			FileOutputStream output = new FileOutputStream(destFile);			
			FileChannel destination = output.getChannel();
        	FileLock fl = destination.tryLock();
			try {
				if(fl.isValid()){
					copyLarge(input, output);
				}else{
					logger.error("{}不能获取独占锁或共享锁,放弃写文件",srcFile.getCanonicalPath());			
				}
			} finally {
				//释放锁
            	if(fl != null && fl.isValid()){
            		fl.release();
            	}
            	if(destination!=null){
                	destination.close();
                	destination = null;
                }           	
				if (output != null) {
					output.close();
				}				
			}
		} finally {
			if (input != null) {
				input.close();
			}
		}
	}
		
	/**
	 * 拷贝文件
	 * @param sourceFile
	 * @param destFile
	 * @deprecated (有些硬件不支持,速度会变慢)
	 */
	public static void copyFile_old(File sourceFile, File destFile) {
				
		//目标文件不存在,先建立
		if(!destFile.isFile()){
			try {
				destFile.getParentFile().mkdirs();
				destFile.createNewFile();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		
		if(sourceFile.length() < FileUtil.freeMemory()){
			FileUtil.copyBigFile_old(sourceFile,destFile);
        }else{
        	FileChannel source = null;
    		FileChannel destination = null;
    		FileLock fl = null;
    		try {
    			source = new FileInputStream(sourceFile).getChannel();
    			destination = new FileOutputStream(destFile).getChannel();
    			fl = destination.tryLock();
    			//增加独占锁定
    			if(fl.isValid()){
    				destination.transferFrom(source, 0, source.size());		
    			}else{
    				logger.error("{}不能获取独占锁或共享锁,放弃写文件",sourceFile.getCanonicalPath());
    			}
    			fl.release();
    		} catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
            	try {
            		if (source != null) {
    				source.close();
            		}
	    			if (destination != null) {
	    				destination.close();
	    			}
	    			if (fl.isValid()) {
	    				fl.release();
	    			}
    			} catch (IOException e) {
    				throw new RuntimeException(e);
                } 			
    			source = null;
        		destination = null;
    		}
        }	
	}
	
	/**
	 * 拷贝大文件
	 * @param sourceFile
	 * @param destFile
	 * @deprecated (有些硬件不支持,速度会变慢)
	 */
	private static void copyBigFile_old(File sourceFile, File destFile){
        
        FileChannel source = null;
        FileChannel destination = null;
        ByteBuffer buffer = null;
        FileLock fl = null;
        try {
        	source = new FileInputStream(sourceFile).getChannel();
        	destination = new FileOutputStream(destFile).getChannel();
            
            /* 每次读取数据的缓存大小 */
        	buffer = ByteBuffer.allocate((new Long((long)(FileUtil.freeMemory() * (1.8f/3f)))).intValue()); 
        	fl = destination.tryLock();
			//增加独占锁定
			if(fl.isValid()){
				while(source.read(buffer) != -1){
		                buffer.flip();
		                destination.write(buffer);
		                buffer.clear();
		        }
			}else{
				logger.error("{}不能获取独占锁或共享锁,放弃写文件",sourceFile.getCanonicalPath());
			}
			//释放锁
			fl.release();     
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }finally{
            // 关闭所有IO对象
            try {
                if(source!=null){
                	source.close();
                	source = null;
                }
                if(destination!=null){
                	destination.close();
                	destination = null;
                }
                if(buffer!=null){
                    buffer.clear();
                    buffer = null;
                }
                if (fl.isValid()) {
    				fl.release();
    			}
            } catch (IOException e) {
                throw new RuntimeException(e);
            }    
            source = null;
            destFile = null;
        }
    }
	
	/**
	 * 通道拷贝NIO_Transfer
	 * @param sourceFile
	 * @param destFile
	 * @deprecated 在某些系统上效率低
	 */
	public static void copyFileNio(File sourceFile, File destFile){
		FileChannel source = null;
		FileChannel destination = null;
		FileLock fl = null;
		try {
			source = new FileInputStream(sourceFile).getChannel();
			destination = new FileOutputStream(destFile).getChannel();
			fl = destination.tryLock();
			//增加独占锁定
			if(fl.isValid()){
				destination.transferFrom(source, 0, source.size());
			}else{
				logger.error("{}不能获取独占锁或共享锁,放弃写文件",sourceFile.getCanonicalPath());
			}
		} catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
        	try {
        		//释放锁
            	if(fl != null && fl.isValid()){
            		fl.release();
            	}
        		if (source != null) {
        			source.close();
        		}
    			if (destination != null) {
    				destination.close();
    			}
			} catch (IOException e) {
				throw new RuntimeException(e);
            }
		}
	}
	
	/**
	 * 拷贝大文件
	 * @param sourceFile
	 * @param destFile
	 * @deprecated 在某些系统上效率低
	 */
	public static void copyBigFile(File sourceFile, File destFile){
        
        FileChannel source = null;
        FileChannel destination = null;
        ByteBuffer buffer = null;
        FileLock fl = null;
        try {
        	source = new FileInputStream(sourceFile).getChannel();
        	destination = new FileOutputStream(destFile).getChannel();
            
            /* 每次读取数据的缓存大小 */
        	//buffer = ByteBuffer.allocate((new Long((long)(MyFileUtil.freeMemory() * (2f/3f)))).intValue());
        	//buffer = ByteBuffer.allocateDirect((new Long((long)(MyFileUtil.freeMemory() * (2f/3f)))).intValue());        	
        	buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);       	
        	fl = destination.tryLock();
			//增加独占锁定
			if(fl.isValid()){
				while(source.read(buffer) != -1){
		                buffer.flip();
		                destination.write(buffer);
		                buffer.clear();
		        }
			}else{
				logger.error("{}不能获取独占锁或共享锁,放弃写文件",sourceFile.getCanonicalPath());
			}
			    
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }finally{
            // 关闭所有IO对象
            try {
            	//释放锁
            	if(fl != null && fl.isValid()){
            		fl.release();
            	}
                if(source!=null){
                	source.close();
                	source = null;
                }
                if(destination!=null){
                	destination.close();
                	destination = null;
                }
                if(buffer!=null){
                    buffer.clear();
                    buffer = null;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
	
	/**
	 * 剪切文件
	 * @param srcFile
	 * @param destFile
	 */
	public static void cutFile(String sourceFile, String destFile) {
		cutFile(new File(sourceFile), new File(destFile));
	}

	/**
	 * 剪切文件
	 * @param sourceFile
	 * @param destFile
	 */
	public static void cutFile(File sourceFile, File destFile) {
		copyFile(sourceFile, destFile);
		sourceFile.delete();
	}
		
	/**
	 * 建立目录
	 * @param fileDir
	 * @return
	 */
	public static boolean makeDirs(String fileDir) {		
		return new File(fileDir).mkdirs();
	}
	
	/**
	 * 批量剪切文件到同一目录
	 * @param destDir
	 * @param sourceDir
	 * @param cutFiles
	 */
	public static void cutFilesToDirectory(File[] cutFiles, String destDir){
		
		for(File f : cutFiles){
			File destFile = new File(destDir + File.separator + f.getName());
			if(destFile.isFile()){
				destFile.delete();
			}
			cutFile(f, destFile);
		}		
	}
		
	/**
	 * 清空文件夹中的所有文件
	 * @param destDir
	 */
	public static void delDir(String destDir){
		delDir(new File(destDir));
	}
	
	/**
	 * 清空文件夹中的所有文件
	 * @param destDir
	 */
	public static void delDir(File destDir){
		
		if(!destDir.delete()){
			if(destDir.isDirectory()){
				File[] f = destDir.listFiles();
				for(File delf : f){
					if(delf.isDirectory()){
						delDir(delf);
					}else{
						delf.delete();
					}
				}
				destDir.delete();
			}
		}
	}
	
	/**
	 * 拷贝文件夹(文件下所有文件)
	 * @param srcDirectory
	 * @param destDirectory
	 */
	public static void copyDir(String srcDir, String destDir){    
        copyDir(new File(srcDir),new File(destDir));
    }
    
	/**
	 * 拷贝文件夹(文件下所有文件)
	 * @param srcDir
	 * @param destDir
	 */
    public static void copyDir(File srcDir, File destDir){        
        
        /* 得到目录下的文件和目录数组 */
        File[] fileList = srcDir.listFiles();      
        for (File srcf : fileList) {
            if (srcf.isFile()) {
                if (!destDir.exists()) {
                	destDir.mkdirs();
                }
                File destf = new File(destDir.getAbsolutePath() + File.separatorChar + srcf.getName());
                //如果目标文件夹存在此文件,先删除
                destf.delete();
                //拷贝文件
                copyFile(srcf,destf);  
            } else {
            	//数组中的对象为目录,如果该子目录在目标文件夹中不存在就创建
                File subDir = new File(destDir.getAbsolutePath() + File.separatorChar + srcf.getName());
                if (!subDir.exists()) {
                    subDir.mkdirs();
                }
                //递归调用自己
                copyDir(srcf,subDir);
            }
        }
        fileList = null;
    }
    
    /**
     * 剪切目录
     * @param srcDir
     * @param destDir
     */
    public static void cutDir(String srcDir, String destDir){
    	cutDir(new File(srcDir),new File(destDir));
    }
    
    /**
     * 剪切目录
     * @param srcDir
     * @param destDir
     */
    public static void cutDir(File srcDir, File destDir){
        copyDir(srcDir,destDir);
        srcDir.delete();
    }
	
	/**
	 * 写内容到文件中
	 * @param file
	 * @param fileContent
	 * @param tag(如果为真,续写文件)
	 * @return
	 */
	public static boolean writeStringToFile(File file, String fileContent, boolean tag) {
		FileWriter fileWriter = null;
		try {
			if (!new File(file.getParent()).exists()) {
				new File(file.getParent()).mkdirs();
				logger.warn("目录{}不存在,程序建立目录",file.getAbsolutePath());
				//file.createNewFile();
			}
			fileWriter = new FileWriter(file, tag);
			fileWriter.write(fileContent);
			fileWriter.flush();
			fileWriter.close();
			//logger.debug("记录文件{}开始记录操作成功的文件名:{}",file.getAbsolutePath(),fileContent);
		} catch (IOException e) {
			logger.error("记录操作成功文件异常", e);
			return false;
		} finally{
			if(fileWriter != null){
				try {
					fileWriter.close();
				} catch (IOException e) {
					logger.error("关闭FileWriter异常", e);
				}
			}
		}
		return true;
	}
	
	/**
	 * 读取文件
	 * @param destFile
	 * @return
	 */
	public static String readFile(String destFile){
		return decodeByteBuffer(readFileToByteBuffer(new File(destFile)), Encode_GBK);
	}
	
	/**
	 * 读取文件
	 * @param destFile
	 * @return
	 */
	public static String readFile(File destFile){
		return decodeByteBuffer(readFileToByteBuffer(destFile), Encode_GBK);
	}
	
	/**
	 * 读取文件
	 * @param destDir
	 * @return
	 */
	public static String readFile(File destFile, String encode){	
		return decodeByteBuffer(readFileToByteBuffer(destFile), encode);		
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
			logger.error("递归查找根目录{}不是一个文件夹,递归查找文件失败",srcDir);
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
		
	/**
	 * 编码ByteBuffer
	 * 
	 * @param buffer
	 * @param encode
	 * @return
	 */
	public static String decodeByteBuffer(ByteBuffer buffer, String encode){
		Charset charset = null;
		CharsetDecoder decoder = null;
		CharBuffer charBuffer = null;
		try {
			charset = Charset.forName(encode);
			decoder = charset.newDecoder();
			charBuffer = decoder.decode(buffer);
			return charBuffer.toString();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * 读取文件(ByteBuffer)
	 * 对于大多数操作系统而言，与通过普通的 read 和 write 方法读取或写入数千字节的数据相比，将文件映射到内存中开销更大。
	 * 从性能的观点来看，通常将相对较大的文件映射到内存中才是值得的。
	 * @param destFile
	 * @return
	 */
	public static ByteBuffer readFileToByteBuffer(File destFile){	
		FileChannel source = null;
		MappedByteBuffer mBuf = null;
		try {
			source = new FileInputStream(destFile).getChannel();		
			mBuf = source.map(FileChannel.MapMode.READ_ONLY, 0, source.size());
		} catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
        	try {
        		if (source != null) {
				source.close();
        		}
			} catch (IOException e) {
				throw new RuntimeException(e);
            } 			
			source = null;
		}
        return mBuf;
	}
	
	/**
	 * 读取String按照给定的Delimiter
	 * @param destFile
	 * @param delimiter
	 * @return
	 */
	public static String[] readFileByDelimiter(File destFile, String delimiter){
		String content = readFile(destFile);
		if(StringUtils.isEmpty(content)){
			return null;
		}
		return content.split(delimiter);
	}
	
	/**
	 * 读取String按照给定的Delimiter
	 * @param destFiles
	 * @param delimiter
	 * @return
	 */
	public static String[] readFilesByDelimiter(File[] destFiles, String delimiter){	
		String content = "";	
		for(File f : destFiles){
			content += readFile(f);
		}
		if(StringUtils.isEmpty(content)){
			return null;
		}
		return content.split(delimiter);
	}
	
	/**
	 * 读取String按照给定的Delimiter(返回ArrayList<String>)
	 * @param destFiles
	 * @param delimiter
	 * @return
	 */
	public static ArrayList<String> readFilesByDelimiterToArrayList(File[] destFiles, String delimiter){
		ArrayList<String> arrayList = new ArrayList<String>();
		
		String content = "";	
		for(File f : destFiles){
			content += readFile(f);
		}
		if(StringUtils.isEmpty(content)){
			return arrayList;
		}
		String[] split = content.split(delimiter);
			
		for(String s : split){
			arrayList.add(s);
		}
		return arrayList;
	}
	
	/**
	 * 根据URL获取文件
	 * @param source
	 * @param destination
	 * @throws IOException
	 */
	public static void copyURLToFile(URL source, File destination) throws IOException {
        InputStream input = source.openStream();
        try {
            FileOutputStream output = openOutputStream(destination, false);
            try {
                copy(input, output);
            } finally {
                closeQuietly(output);
            }
        } finally {
            closeQuietly(input);
        }
    }
	
	/**
	 * 打开输出流
	 * @param file
	 * @param append
	 * @return
	 * @throws IOException
	 */
	public static FileOutputStream openOutputStream(File file, boolean append) throws IOException {
        if (file.exists()) {
            if (file.isDirectory()) {
                throw new IOException("文件 '" + file + "' 存在,但是一个目录,非文件");
            }
            if (file.canWrite() == false) {
                throw new IOException("文件 '" + file + "' 不能被写");
            }
        } else {
            File parent = file.getParentFile();
            if (parent != null && parent.exists() == false) {
                if (parent.mkdirs() == false) {
                    throw new IOException("文件 '" + file + "' 不能创建");
                }
            }
        }
        return new FileOutputStream(file,append);
    }
	
	/**
	 * 打开输入流
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static FileInputStream openInputStream(File file) throws IOException {
        if (file.exists()) {
            if (file.isDirectory()) {
                throw new IOException("文件 '" + file + "' 存在,但是一个目录,非文件");
            }
            if (file.canRead() == false) {
                throw new IOException("文件 '" + file + "' 不能被读");
            }
        } else {
            throw new FileNotFoundException("文件 '" + file + "' 不存在");
        }
        return new FileInputStream(file);
    }
	
	/**
	 * 拷贝文件输入流到输出流
	 * @param input
	 * @param output
	 * @return
	 * @throws IOException
	 */
	public static int copy(InputStream input, OutputStream output) throws IOException {
        long count = copyLarge(input, output);
        if (count > Integer.MAX_VALUE) {
            return -1;
        }
        return (int) count;
    }
	
	/**
	 * 拷贝大文件
	 * @param input
	 * @param output
	 * @return
	 * @throws IOException
	 */
	public static long copyLarge(InputStream input, OutputStream output)
			throws IOException {
		byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
		long count = 0;
		int n = 0;
		while (-1 != (n = input.read(buffer))) {
			output.write(buffer, 0, n);
			count += n;
		}
		return count;
	}
	
	/**
	 * 关闭输出流
	 * @param output
	 */
	public static void closeQuietly(OutputStream output) {
        try {
            if (output != null) {
                output.close();
            }
        } catch (IOException ioe) {
            // ignore
        }
    }
	
	/**
	 * 关闭输入流
	 * @param input
	 */
	public static void closeQuietly(InputStream input) {
        try {
            if (input != null) {
                input.close();
            }
        } catch (IOException ioe) {
            // ignore
        }
    }
	
	/**
	 * 比较文件日期
	 * @param file
	 * @param reference
	 * @return
	 */
	public static boolean isFileNewer(File file, File reference) {
        if (reference == null) {
            throw new IllegalArgumentException("没有指定要比较的文件");
        }
        if (!reference.exists()) {
            throw new IllegalArgumentException("比较文件 '"
                    + file + "' 不存在");
        }
        return isFileNewer(file, reference.lastModified());
    }
	
	/**
	 * 比较文件日期
	 * @param file
	 * @param date
	 * @return
	 */
	public static boolean isFileNewer(File file, Date date) {
        if (date == null) {
            throw new IllegalArgumentException("没有指定日期");
        }
        return isFileNewer(file, date.getTime());
    }
	
	
	/**
	 * 比较文件修改时间
	 * @param file
	 * @param timeMillis
	 * @return
	 */
	public static boolean isFileNewer(File file, long timeMillis) {
        if (file == null) {
            throw new IllegalArgumentException("文件没有指定");
        }
        if (!file.exists()) {
            return false;
        }
        return file.lastModified() > timeMillis;
    }
	
	
	public static List<String> readLines(String file) throws IOException{
		return readLines(new File(file), null);
	}
	
	/**
	 * 读取行
	 * @param file
	 * @param encoding
	 * @return
	 * @throws IOException
	 */
	public static List<String> readLines(File file, String encoding) throws IOException {
        InputStream in = null;
        try {
            in = openInputStream(file);
            return readLines(in, encoding);
        } finally {
            closeQuietly(in);
        }
    }
	
	/**
	 * 读取行
	 * @param input
	 * @param encoding
	 * @return
	 * @throws IOException
	 */
	public static List<String> readLines(InputStream input, String encoding) throws IOException {
        if (encoding == null) {
            return readLines(input);
        } else {
            InputStreamReader reader = new InputStreamReader(input, encoding);
            return readLines(reader);
        }
    }
	
	/**
	 * 读取行
	 * @param input
	 * @return
	 * @throws IOException
	 */
	public static List<String> readLines(InputStream input) throws IOException {
        InputStreamReader reader = new InputStreamReader(input);
        return readLines(reader);
    }
	
	/**
	 * 读取行
	 * @param input
	 * @return
	 * @throws IOException
	 */
	public static List<String> readLines(Reader input) throws IOException {
        BufferedReader reader = new BufferedReader(input);
        List<String> list = new ArrayList<String>();
        String line = reader.readLine();
        while (line != null) {
            list.add(line);
            line = reader.readLine();
        }
        return list;
    }
	
	/**
	 * String写到File中(NIO)
	 * @param data
	 * @param destFile
	 * @param append 续写
	 * @throws IOException
	 * @deprecated with bug(Big data)
	 */
	public static void writeStringToFileNIO(String data, File destFile, boolean append) throws IOException{
		writeStringToFileNIO(data, destFile, null, append);
	}
		
	/**
	 * String写到File中(NIO)
	 * @param data
	 * @param destFile
	 * @param encoding
	 * @param append 续写
	 * @throws IOException
	 * @deprecated with bug(Big data)
	 */
	public static void writeStringToFileNIO(String data, File destFile, String encoding, boolean append) throws IOException{
		
		FileOutputStream file = openOutputStream(destFile, append);
	    FileChannel outChannel = file.getChannel();

	    ByteBuffer buf = ByteBuffer.allocateDirect(data.length());
	    byte[] bytes = null;
	    
	    if(StringUtils.isEmpty(encoding)){
	    	bytes = data.getBytes();
	    }else{
	    	bytes = data.getBytes(encoding);
	    }
	    
	    buf.put(bytes);
	    buf.flip();
	    try {
	      outChannel.write(buf);
	      file.close();
	    } catch (IOException e) {
	      logger.error("NIO写文件异常", e);
	    } finally {
	    	file.close();
	    }
	}
	
	/**
	 * @param sourceFile
	 * @param lineNum
	 * @return
	 * @throws IOException
	 * @deprecated (Error Don't Use It)
	 */
	public static List<String> readLinesByLineNum(File sourceFile, int startLineNum, int num) throws IOException{
		FileReader in = new FileReader(sourceFile);
        LineNumberReader reader = new LineNumberReader(in);
        List<String> list = new ArrayList<String>();
        
        String line = reader.readLine();
        int lineNum = 0;
        reader.setLineNumber(startLineNum);
        
        while(lineNum < num){
        	lineNum++;
        	if(line != null) {
                list.add(line);
                line = reader.readLine();
            }
        }
        reader.close();
        in.close();       
        return list;
	}
	
	/**
	 * @param sourceFile
	 * @param lineNum
	 * @return
	 * @throws IOException
	 */
	public static HashMap<Long,List<String>> readLinesByPos(File sourceFile, long pos, int num) throws IOException{
		HashMap<Long,List<String>> hm = new HashMap<Long,List<String>>(1);
        List<String> list = new ArrayList<String>();
        BufferedRandomAccessFile braf = new BufferedRandomAccessFile(sourceFile,"r");
        braf.seek(pos);
        int i = 0;
        String line = null;
        while(((line = braf.readLine()) != null) && (i < num)){     	
        	list.add(line);
        	i++;
        }
        hm.put(braf.getFilePointer(), list);
        if(braf != null){
        	braf.close();
        }
        return hm;
	}
	
	
	/**
	 * 写入行
	 * @param file
	 * @param lines
	 * @throws IOException
	 */
	public static void writeLines(File file, Collection<String> lines) throws IOException{
		writeLines(file, null, lines, null, false);
	}
	
	/**
	 * 写入行
	 * @param file
	 * @param encoding
	 * @param lines
	 * @param lineEnding
	 * @param append 续写
	 * @throws IOException
	 */
	public static void writeLines(File file, String encoding,
			Collection<String> lines, String lineEnding, boolean append) throws IOException {
		OutputStream out = null;
		try {
			out = openOutputStream(file, append);
			writeLines(lines, lineEnding, out, encoding);
		} finally {
			closeQuietly(out);
		}
	}
	
	
	/**
	 * 写入行
	 * @param lines
	 * @param lineEnding
	 * @param output
	 * @param encoding
	 * @throws IOException
	 */
	public static void writeLines(Collection<String> lines, String lineEnding,
			OutputStream output, String encoding) throws IOException {
		if (encoding == null) {
			writeLines(lines, lineEnding, output);
		} else {
			if (lines == null) {
				return;
			}
			if (lineEnding == null) {
				lineEnding = LINE_SEPARATOR;
			}
			for (Iterator<?> it = lines.iterator(); it.hasNext();) {
				Object line = it.next();
				if (line != null) {
					output.write(line.toString().getBytes(encoding));
				}
				output.write(lineEnding.getBytes(encoding));
			}
		}
	}
	
	
	/**
	 * 写入行
	 * @param lines
	 * @param lineEnding
	 * @param output
	 * @throws IOException
	 */
	public static void writeLines(Collection<?> lines, String lineEnding,
			OutputStream output) throws IOException {
		if (lines == null) {
			return;
		}
		if (lineEnding == null) {
			lineEnding = LINE_SEPARATOR;
		}
		for (Iterator<?> it = lines.iterator(); it.hasNext();) {
			Object line = it.next();
			if (line != null) {
				output.write(line.toString().getBytes());
			}
			output.write(lineEnding.getBytes());
		}
	}
	
	/**
	 * 获取文件夹大小(byte)
	 * @param directory
	 * @return
	 */
	public static long sizeOfDirectory(File directory) {
        if (!directory.exists()) {
            String message = directory + " 不存在";
            throw new IllegalArgumentException(message);
        }

        if (!directory.isDirectory()) {
            String message = directory + " 不是一个文件夹";
            throw new IllegalArgumentException(message);
        }

        long size = 0;

        File[] files = directory.listFiles();
        if (files == null) {  //安全问题
            return 0L;
        }
        for (int i = 0; i < files.length; i++) {
            File file = files[i];

            if (file.isDirectory()) {
                size += sizeOfDirectory(file);
            } else {
                size += file.length();
            }
        }
        return size;
    }
	
	/**
	 * 获取文件组大小(byte)
	 * @param directory
	 * @return
	 */
	public static long sizeOfFiles(File[] files) {
        long size = 0;
        if (files == null) {  //安全问题
            return 0L;
        }
        for (int i = 0; i < files.length; i++) {
            File file = files[i];
            if(file.exists()){
            	if (file.isDirectory()) {
                    size += sizeOfDirectory(file);
                } else {
                    size += file.length();
                }
            }            
        }
        return size;
    }
	
	/**
	 * 格式化文件大小
	 * @param size
	 * @return
	 */
	public static String formatSize(float size){
		long kb = 1024;
		long mb = (kb * 1024);
		long gb = (mb * 1024);
		if (size < kb) {
			return String.format("%d B", (int) size);
		}
		else if (size < mb) {
			return String.format("%.2f KB", size / kb); // 保留两位小数
		}
		else if (size < gb) {
			return String.format("%.2f MB", size / mb);
		}
		else {
			return String.format("%.2f GB", size / gb);
		}
	}
	
	public static void main(String[] args) throws IOException {		
	}
	
}
